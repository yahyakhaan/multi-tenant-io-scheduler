#include "ioshield/file_block_device.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace ioshield {
namespace {

bool request_in_range(const IORequest& request, std::uint64_t device_size) {
    if (request.size_bytes == 0) {
        return false;
    }
    const auto size = static_cast<std::uint64_t>(request.size_bytes);
    return request.offset_bytes <= device_size && size <= device_size - request.offset_bytes;
}

std::byte pattern_byte(std::uint64_t request_id, std::size_t index) {
    const auto mixed = request_id * 0x9e3779b97f4a7c15ULL + static_cast<std::uint64_t>(index);
    return static_cast<std::byte>((mixed ^ (mixed >> 32U)) & 0xffU);
}

bool write_all(int descriptor, const std::byte* data, std::size_t size, std::uint64_t offset) {
    std::size_t written = 0;
    while (written < size) {
        const auto result = ::pwrite(
            descriptor,
            data + written,
            size - written,
            static_cast<off_t>(offset + written));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        written += static_cast<std::size_t>(result);
    }
    return true;
}

bool read_all(int descriptor, std::byte* data, std::size_t size, std::uint64_t offset) {
    std::size_t read = 0;
    while (read < size) {
        const auto result = ::pread(
            descriptor,
            data + read,
            size - read,
            static_cast<off_t>(offset + read));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        read += static_cast<std::size_t>(result);
    }
    return true;
}

}  // namespace

FileBlockDevice::FileBlockDevice(FileDeviceConfig config)
    : config_(std::move(config)) {
    if (config_.path.empty()) {
        throw std::invalid_argument("file device path must not be empty");
    }
    if (config_.size_bytes == 0 || config_.lanes == 0 || config_.queue_depth == 0) {
        throw std::invalid_argument("file size, lanes, and queue depth must be positive");
    }
    if (config_.size_bytes > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
        throw std::invalid_argument("file device size exceeds platform offset range");
    }

    file_descriptor_ = ::open(config_.path.c_str(), O_CREAT | O_RDWR, 0644);
    if (file_descriptor_ < 0) {
        throw std::system_error(errno, std::generic_category(), "open file-backed device");
    }
    if (::ftruncate(file_descriptor_, static_cast<off_t>(config_.size_bytes)) != 0) {
        const int error = errno;
        ::close(file_descriptor_);
        file_descriptor_ = -1;
        throw std::system_error(error, std::generic_category(), "resize file-backed device");
    }

    workers_.reserve(config_.lanes);
    for (std::size_t i = 0; i < config_.lanes; ++i) {
        workers_.emplace_back([this] { worker_loop(); });
    }
}

FileBlockDevice::~FileBlockDevice() {
    close();
}

bool FileBlockDevice::submit(IORequest request, CompletionCallback completion) {
    {
        std::lock_guard lock(mutex_);
        if (closed_) {
            return false;
        }
    }

    const auto dispatched_at = Clock::now();
    if (!request_in_range(request, config_.size_bytes)) {
        ++submitted_;
        ++completed_;
        ++errors_;
        completion(IOCompletion{
            .request = std::move(request),
            .status = IOStatus::InvalidRequest,
            .dispatched_at = dispatched_at,
            .completed_at = Clock::now(),
        });
        return true;
    }

    std::unique_lock lock(mutex_);
    capacity_available_.wait(lock, [this] {
        return closed_ || in_flight_ < config_.queue_depth;
    });
    if (closed_) {
        return false;
    }

    jobs_.push_back(Job{std::move(request), Clock::now(), std::move(completion)});
    ++in_flight_;
    ++submitted_;
    lock.unlock();
    jobs_ready_.notify_one();
    return true;
}

void FileBlockDevice::wait_idle() {
    std::unique_lock lock(mutex_);
    idle_.wait(lock, [this] { return in_flight_ == 0; });
}

void FileBlockDevice::close() {
    {
        std::lock_guard lock(mutex_);
        closed_ = true;
    }
    jobs_ready_.notify_all();
    capacity_available_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    if (file_descriptor_ >= 0) {
        ::close(file_descriptor_);
        file_descriptor_ = -1;
    }
}

DeviceStats FileBlockDevice::stats() const {
    std::lock_guard lock(mutex_);
    return DeviceStats{
        .submitted = submitted_.load(),
        .completed = completed_.load(),
        .errors = errors_.load(),
        .busy_time_ns = busy_time_ns_.load(),
        .in_flight = in_flight_,
        .lanes = config_.lanes,
    };
}

IOStatus FileBlockDevice::execute(const IORequest& request) const {
    std::vector<std::byte> buffer(request.size_bytes);
    if (request.operation == IOOperation::Write) {
        for (std::size_t i = 0; i < buffer.size(); ++i) {
            buffer[i] = pattern_byte(request.id, i);
        }
        if (!write_all(file_descriptor_, buffer.data(), buffer.size(), request.offset_bytes)) {
            return IOStatus::IOError;
        }
        if (config_.durability == FileDurability::FsyncPerWrite &&
            ::fsync(file_descriptor_) != 0) {
            return IOStatus::IOError;
        }
        return IOStatus::Ok;
    }

    return read_all(file_descriptor_, buffer.data(), buffer.size(), request.offset_bytes)
        ? IOStatus::Ok
        : IOStatus::IOError;
}

void FileBlockDevice::worker_loop() {
    while (true) {
        Job job;
        {
            std::unique_lock lock(mutex_);
            jobs_ready_.wait(lock, [this] { return closed_ || !jobs_.empty(); });
            if (jobs_.empty()) {
                return;
            }
            job = std::move(jobs_.front());
            jobs_.pop_front();
        }

        const auto service_started = Clock::now();
        const auto status = execute(job.request);
        const auto service_finished = Clock::now();
        ++completed_;
        if (status != IOStatus::Ok) {
            ++errors_;
        }
        busy_time_ns_.fetch_add(
            static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                service_finished - service_started).count()));
        job.completion(IOCompletion{
            .request = std::move(job.request),
            .status = status,
            .dispatched_at = job.dispatched_at,
            .completed_at = service_finished,
        });

        {
            std::lock_guard lock(mutex_);
            --in_flight_;
            if (in_flight_ == 0) {
                idle_.notify_all();
            }
        }
        capacity_available_.notify_one();
    }
}

}  // namespace ioshield
