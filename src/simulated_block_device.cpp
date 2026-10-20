#include "ioshield/simulated_block_device.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <utility>

namespace ioshield {
namespace {

constexpr double kBytesPerMebibyte = 1024.0 * 1024.0;

double deterministic_unit_jitter(std::uint64_t value) {
    value ^= value >> 30U;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27U;
    value *= 0x94d049bb133111ebULL;
    value ^= value >> 31U;
    const double normalized = static_cast<double>(value) /
        static_cast<double>(std::numeric_limits<std::uint64_t>::max());
    return normalized * 2.0 - 1.0;
}

}  // namespace

SimulatedBlockDevice::SimulatedBlockDevice(SimulatedDeviceConfig config)
    : config_(config) {
    if (config_.max_iops <= 0.0 || config_.max_bandwidth_mib_per_sec <= 0.0) {
        throw std::invalid_argument("device IOPS and bandwidth must be positive");
    }
    if (config_.lanes == 0 || config_.queue_depth == 0) {
        throw std::invalid_argument("device lanes and queue depth must be positive");
    }
    if (config_.jitter_fraction < 0.0 || config_.jitter_fraction > 1.0) {
        throw std::invalid_argument("jitter fraction must be between zero and one");
    }
    if (config_.write_cost_multiplier <= 0.0) {
        throw std::invalid_argument("write cost multiplier must be positive");
    }

    workers_.reserve(config_.lanes);
    for (std::size_t i = 0; i < config_.lanes; ++i) {
        workers_.emplace_back([this] { worker_loop(); });
    }
}

SimulatedBlockDevice::~SimulatedBlockDevice() {
    close();
}

bool SimulatedBlockDevice::submit(IORequest request, CompletionCallback completion) {
    std::unique_lock lock(mutex_);
    capacity_available_.wait(lock, [this] {
        return closed_ || in_flight_ < config_.queue_depth;
    });

    if (closed_) {
        return false;
    }

    jobs_.push_back(Job{std::move(request), Clock::now(), std::move(completion)});
    ++in_flight_;
    lock.unlock();
    jobs_ready_.notify_one();
    return true;
}

void SimulatedBlockDevice::wait_idle() {
    std::unique_lock lock(mutex_);
    idle_.wait(lock, [this] { return in_flight_ == 0; });
}

void SimulatedBlockDevice::close() {
    {
        std::lock_guard lock(mutex_);
        if (closed_) {
            return;
        }
        closed_ = true;
    }

    jobs_ready_.notify_all();
    capacity_available_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

std::chrono::nanoseconds SimulatedBlockDevice::service_time(const IORequest& request) const {
    const double lanes = static_cast<double>(config_.lanes);
    const double iops_seconds = lanes / config_.max_iops;
    const double bandwidth_bytes_per_second =
        config_.max_bandwidth_mib_per_sec * kBytesPerMebibyte;
    const double bandwidth_seconds =
        (static_cast<double>(request.size_bytes) * lanes) / bandwidth_bytes_per_second;
    const double capacity_seconds = std::max(iops_seconds, bandwidth_seconds);
    const double operation_multiplier = request.operation == IOOperation::Write
        ? config_.write_cost_multiplier
        : 1.0;
    const double base_seconds =
        std::chrono::duration<double>(config_.base_latency).count();
    const double jitter = 1.0 +
        config_.jitter_fraction * deterministic_unit_jitter(request.id);
    const double seconds = std::max(0.0, std::max(base_seconds, capacity_seconds) *
        operation_multiplier * jitter);
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::duration<double>(seconds));
}

void SimulatedBlockDevice::worker_loop() {
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

        std::this_thread::sleep_for(service_time(job.request));
        IOCompletion result{
            .request = std::move(job.request),
            .status = IOStatus::Ok,
            .dispatched_at = job.dispatched_at,
            .completed_at = Clock::now(),
        };
        job.completion(std::move(result));

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

