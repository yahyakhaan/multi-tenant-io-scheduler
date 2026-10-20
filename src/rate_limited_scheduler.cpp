#include "ioshield/rate_limited_scheduler.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>

namespace ioshield {
namespace {

void validate_limit(const TenantRateLimit& limit) {
    if (limit.tenant_id.empty()) {
        throw std::invalid_argument("tenant ID must not be empty");
    }
    const bool valid = std::isfinite(limit.iops_per_second) && limit.iops_per_second > 0.0 &&
        std::isfinite(limit.operation_burst) && limit.operation_burst >= 1.0 &&
        std::isfinite(limit.bytes_per_second) && limit.bytes_per_second > 0.0 &&
        std::isfinite(limit.byte_burst) && limit.byte_burst >= 1.0;
    if (!valid) {
        throw std::invalid_argument("tenant rates must be positive and operation burst must be at least one");
    }
}

}  // namespace

RateLimitedScheduler::TenantState::TenantState(
    const TenantRateLimit& limit,
    Clock::time_point initial_time)
    : limiter(
          limit.iops_per_second,
          limit.operation_burst,
          limit.bytes_per_second,
          limit.byte_burst,
          initial_time) {}

RateLimitedScheduler::RateLimitedScheduler(
    std::vector<TenantRateLimit> limits,
    Clock::time_point initial_time) {
    if (limits.empty()) {
        throw std::invalid_argument("at least one tenant rate limit is required");
    }

    tenant_order_.reserve(limits.size());
    for (const auto& limit : limits) {
        validate_limit(limit);
        const auto [iterator, inserted] = tenants_.try_emplace(
            limit.tenant_id,
            limit,
            initial_time);
        static_cast<void>(iterator);
        if (!inserted) {
            throw std::invalid_argument("duplicate tenant rate limit: " + limit.tenant_id);
        }
        tenant_order_.push_back(limit.tenant_id);
    }
}

bool RateLimitedScheduler::enqueue(IORequest request) {
    {
        std::lock_guard lock(mutex_);
        if (closed_) {
            return false;
        }
        auto tenant = tenants_.find(request.tenant_id);
        if (tenant == tenants_.end()) {
            return false;
        }
        if (request.size_bytes == 0 ||
            static_cast<double>(request.size_bytes) > tenant->second.limiter.byte_capacity()) {
            return false;
        }
        tenant->second.queue.push_back(std::move(request));
        ++queued_;
    }
    ready_.notify_one();
    return true;
}

std::optional<IORequest> RateLimitedScheduler::wait_dequeue() {
    std::unique_lock lock(mutex_);

    while (true) {
        if (queued_ == 0) {
            if (closed_) {
                return std::nullopt;
            }
            ready_.wait(lock, [this] { return closed_ || queued_ != 0; });
            continue;
        }

        const auto now = Clock::now();
        std::optional<Clock::time_point> earliest_eligibility;

        for (std::size_t offset = 0; offset < tenant_order_.size(); ++offset) {
            const std::size_t index = (next_tenant_ + offset) % tenant_order_.size();
            auto& tenant = tenants_.at(tenant_order_[index]);
            if (tenant.queue.empty()) {
                continue;
            }

            const auto request_bytes = static_cast<double>(tenant.queue.front().size_bytes);
            if (tenant.limiter.try_consume(1.0, request_bytes, now)) {
                IORequest request = std::move(tenant.queue.front());
                tenant.queue.pop_front();
                --queued_;
                next_tenant_ = (index + 1) % tenant_order_.size();
                return request;
            }

            const auto wait = tenant.limiter.time_until_available(1.0, request_bytes, now);
            if (wait != std::chrono::nanoseconds::max()) {
                const auto eligible_at = now + wait;
                if (!earliest_eligibility || eligible_at < *earliest_eligibility) {
                    earliest_eligibility = eligible_at;
                }
            }
        }

        if (!earliest_eligibility) {
            throw std::logic_error("queued request can never become rate-limit eligible");
        }
        ready_.wait_until(lock, *earliest_eligibility);
    }
}

void RateLimitedScheduler::close() {
    {
        std::lock_guard lock(mutex_);
        closed_ = true;
    }
    ready_.notify_all();
}

std::size_t RateLimitedScheduler::queued() const {
    std::lock_guard lock(mutex_);
    return queued_;
}

}  // namespace ioshield

