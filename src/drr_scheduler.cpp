#include "ioshield/drr_scheduler.hpp"

#include <chrono>
#include <cmath>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>

namespace ioshield {
namespace {

void validate_config(const DrrTenantConfig& config) {
    if (config.tenant_id.empty()) {
        throw std::invalid_argument("tenant ID must not be empty");
    }
    const bool valid = config.weight > 0 &&
        std::isfinite(config.iops_per_second) && config.iops_per_second > 0.0 &&
        std::isfinite(config.operation_burst) && config.operation_burst >= 1.0 &&
        std::isfinite(config.bytes_per_second) && config.bytes_per_second > 0.0 &&
        std::isfinite(config.byte_burst) && config.byte_burst >= 1.0;
    if (!valid) {
        throw std::invalid_argument("DRR tenant weights, rates, and bursts must be positive");
    }
}

std::uint64_t checked_quantum(std::size_t base_quantum, std::uint32_t weight) {
    const auto base = static_cast<std::uint64_t>(base_quantum);
    if (base > std::numeric_limits<std::uint64_t>::max() / weight) {
        throw std::invalid_argument("DRR quantum overflows 64-bit byte accounting");
    }
    return base * weight;
}

}  // namespace

DrrScheduler::TenantState::TenantState(
    const DrrTenantConfig& config,
    std::uint64_t quantum,
    Clock::time_point initial_time)
    : limiter(
          config.iops_per_second,
          config.operation_burst,
          config.bytes_per_second,
          config.byte_burst,
          initial_time),
      quantum_bytes(quantum) {}

DrrScheduler::DrrScheduler(
    std::vector<DrrTenantConfig> tenant_configs,
    std::size_t base_quantum_bytes,
    Clock::time_point initial_time) {
    if (tenant_configs.empty()) {
        throw std::invalid_argument("at least one DRR tenant is required");
    }
    if (base_quantum_bytes == 0) {
        throw std::invalid_argument("DRR base quantum must be positive");
    }

    tenant_order_.reserve(tenant_configs.size());
    for (const auto& config : tenant_configs) {
        validate_config(config);
        const auto quantum = checked_quantum(base_quantum_bytes, config.weight);
        const auto [iterator, inserted] = tenants_.try_emplace(
            config.tenant_id,
            config,
            quantum,
            initial_time);
        static_cast<void>(iterator);
        if (!inserted) {
            throw std::invalid_argument("duplicate DRR tenant: " + config.tenant_id);
        }
        tenant_order_.push_back(config.tenant_id);
    }
}

bool DrrScheduler::enqueue(IORequest request) {
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

std::optional<IORequest> DrrScheduler::wait_dequeue() {
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
        bool request_needs_more_deficit = false;
        std::size_t examined = 0;

        while (examined < tenant_order_.size()) {
            auto& tenant = tenants_.at(tenant_order_[current_tenant_]);
            if (tenant.queue.empty()) {
                tenant.deficit_bytes = 0;
                advance_tenant();
                ++examined;
                continue;
            }

            const auto request_bytes =
                static_cast<std::uint64_t>(tenant.queue.front().size_bytes);
            if (!tenant.limiter.can_consume(
                    1.0,
                    static_cast<double>(request_bytes),
                    now)) {
                const auto wait = tenant.limiter.time_until_available(
                    1.0,
                    static_cast<double>(request_bytes),
                    now);
                if (wait != std::chrono::nanoseconds::max()) {
                    const auto eligible_at = now + wait;
                    if (!earliest_eligibility || eligible_at < *earliest_eligibility) {
                        earliest_eligibility = eligible_at;
                    }
                }
                advance_tenant();
                ++examined;
                continue;
            }

            if (!continuing_visit_) {
                if (std::numeric_limits<std::uint64_t>::max() - tenant.deficit_bytes <
                    tenant.quantum_bytes) {
                    tenant.deficit_bytes = std::numeric_limits<std::uint64_t>::max();
                } else {
                    tenant.deficit_bytes += tenant.quantum_bytes;
                }
                continuing_visit_ = true;
            }

            if (request_bytes > tenant.deficit_bytes) {
                request_needs_more_deficit = true;
                advance_tenant();
                ++examined;
                continue;
            }

            if (!tenant.limiter.try_consume(1.0, static_cast<double>(request_bytes), now)) {
                throw std::logic_error("DRR quota eligibility changed while holding scheduler lock");
            }

            IORequest request = std::move(tenant.queue.front());
            tenant.queue.pop_front();
            tenant.deficit_bytes -= request_bytes;
            --queued_;

            if (tenant.queue.empty()) {
                tenant.deficit_bytes = 0;
                advance_tenant();
            } else if (static_cast<std::uint64_t>(tenant.queue.front().size_bytes) >
                       tenant.deficit_bytes) {
                advance_tenant();
            }
            return request;
        }

        if (request_needs_more_deficit) {
            continue;
        }
        if (!earliest_eligibility) {
            throw std::logic_error("queued DRR request can never become eligible");
        }
        ready_.wait_until(lock, *earliest_eligibility);
    }
}

void DrrScheduler::close() {
    {
        std::lock_guard lock(mutex_);
        closed_ = true;
    }
    ready_.notify_all();
}

std::size_t DrrScheduler::queued() const {
    std::lock_guard lock(mutex_);
    return queued_;
}

void DrrScheduler::advance_tenant() {
    current_tenant_ = (current_tenant_ + 1) % tenant_order_.size();
    continuing_visit_ = false;
}

}  // namespace ioshield

