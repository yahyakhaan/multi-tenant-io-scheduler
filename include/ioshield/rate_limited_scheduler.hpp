#pragma once

#include "ioshield/scheduler.hpp"
#include "ioshield/token_bucket.hpp"

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace ioshield {

struct TenantRateLimit {
    std::string tenant_id;
    double iops_per_second{};
    double operation_burst{};
    double bytes_per_second{};
    double byte_burst{};
};

class RateLimitedScheduler final : public IScheduler {
public:
    explicit RateLimitedScheduler(
        std::vector<TenantRateLimit> limits,
        Clock::time_point initial_time = Clock::now());

    bool enqueue(IORequest request) override;
    std::optional<IORequest> wait_dequeue() override;
    void close() override;
    [[nodiscard]] std::size_t queued() const override;

private:
    struct TenantState {
        TenantState(const TenantRateLimit& limit, Clock::time_point initial_time);

        std::deque<IORequest> queue;
        DualTokenBucket limiter;
    };

    mutable std::mutex mutex_;
    std::condition_variable ready_;
    std::map<std::string, TenantState> tenants_;
    std::vector<std::string> tenant_order_;
    std::size_t next_tenant_{0};
    std::size_t queued_{0};
    bool closed_{false};
};

}  // namespace ioshield

