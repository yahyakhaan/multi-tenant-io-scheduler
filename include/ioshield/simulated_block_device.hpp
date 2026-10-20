#pragma once

#include "ioshield/block_device.hpp"

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

namespace ioshield {

struct SimulatedDeviceConfig {
    double max_iops{5'000.0};
    double max_bandwidth_mib_per_sec{250.0};
    std::chrono::microseconds base_latency{100};
    std::size_t lanes{1};
    std::size_t queue_depth{1};
    double jitter_fraction{0.0};
    double write_cost_multiplier{1.0};
};

class SimulatedBlockDevice final : public IBlockDevice {
public:
    explicit SimulatedBlockDevice(SimulatedDeviceConfig config);
    ~SimulatedBlockDevice() override;

    SimulatedBlockDevice(const SimulatedBlockDevice&) = delete;
    SimulatedBlockDevice& operator=(const SimulatedBlockDevice&) = delete;

    bool submit(IORequest request, CompletionCallback completion) override;
    void wait_idle() override;
    void close() override;

    [[nodiscard]] const SimulatedDeviceConfig& config() const noexcept { return config_; }

private:
    struct Job {
        IORequest request;
        Clock::time_point dispatched_at;
        CompletionCallback completion;
    };

    [[nodiscard]] std::chrono::nanoseconds service_time(const IORequest& request) const;
    void worker_loop();

    SimulatedDeviceConfig config_;
    std::mutex mutex_;
    std::condition_variable jobs_ready_;
    std::condition_variable capacity_available_;
    std::condition_variable idle_;
    std::deque<Job> jobs_;
    std::vector<std::thread> workers_;
    std::size_t in_flight_{0};
    bool closed_{false};
};

}  // namespace ioshield

