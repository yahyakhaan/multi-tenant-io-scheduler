#pragma once

#include "ioshield/block_device.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace ioshield {

enum class FileDurability : std::uint8_t {
    Buffered,
    FsyncPerWrite,
};

struct FileDeviceConfig {
    std::string path;
    std::uint64_t size_bytes{1ULL << 30};
    std::size_t lanes{1};
    std::size_t queue_depth{1};
    FileDurability durability{FileDurability::Buffered};
};

class FileBlockDevice final : public IBlockDevice {
public:
    explicit FileBlockDevice(FileDeviceConfig config);
    ~FileBlockDevice() override;

    FileBlockDevice(const FileBlockDevice&) = delete;
    FileBlockDevice& operator=(const FileBlockDevice&) = delete;

    bool submit(IORequest request, CompletionCallback completion) override;
    void wait_idle() override;
    void close() override;
    [[nodiscard]] DeviceStats stats() const override;

    [[nodiscard]] const FileDeviceConfig& config() const noexcept { return config_; }

private:
    struct Job {
        IORequest request;
        Clock::time_point dispatched_at;
        CompletionCallback completion;
    };

    [[nodiscard]] IOStatus execute(const IORequest& request) const;
    void worker_loop();

    FileDeviceConfig config_;
    int file_descriptor_{-1};
    mutable std::mutex mutex_;
    std::condition_variable jobs_ready_;
    std::condition_variable capacity_available_;
    std::condition_variable idle_;
    std::deque<Job> jobs_;
    std::vector<std::thread> workers_;
    std::size_t in_flight_{0};
    bool closed_{false};
    std::atomic<std::uint64_t> submitted_{0};
    std::atomic<std::uint64_t> completed_{0};
    std::atomic<std::uint64_t> errors_{0};
    std::atomic<std::uint64_t> busy_time_ns_{0};
};

}  // namespace ioshield

