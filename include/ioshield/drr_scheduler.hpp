#pragma once

#include "ioshield/scheduler.hpp"
#include "ioshield/token_bucket.hpp"

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace ioshield {

struct DrrTenantConfig {
    std::string tenant_id;
    std::uint32_t weight{1};
    double iops_per_second{};
    double operation_burst{};
    double bytes_per_second{};
    double byte_burst{};
};

class DrrScheduler final : public IScheduler {
public:
    explicit DrrScheduler(
        std::vector<DrrTenantConfig> tenants,
        std::size_t base_quantum_bytes = 4096,
        Clock::time_point initial_time = Clock::now());

    bool enqueue(IORequest request) override;
    std::optional<IORequest> wait_dequeue() override;
    void close() override;
    [[nodiscard]] std::size_t queued() const override;

private:
    struct TenantState {
        TenantState(
            const DrrTenantConfig& config,
            std::uint64_t quantum_bytes,
            Clock::time_point initial_time);

        std::deque<IORequest> queue;
        DualTokenBucket limiter;
        std::uint64_t quantum_bytes{};
        std::uint64_t deficit_bytes{};
    };

    void advance_tenant();

    mutable std::mutex mutex_;
    std::condition_variable ready_;
    std::map<std::string, TenantState> tenants_;
    std::vector<std::string> tenant_order_;
    std::size_t current_tenant_{0};
    std::size_t queued_{0};
    bool continuing_visit_{false};
    bool closed_{false};
};

}  // namespace ioshield

