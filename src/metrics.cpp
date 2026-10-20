#include "ioshield/metrics.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace ioshield {
namespace {

double percentile_us(std::vector<std::uint64_t> values, double percentile) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const double rank = percentile * static_cast<double>(values.size() - 1);
    const auto lower = static_cast<std::size_t>(std::floor(rank));
    const auto upper = static_cast<std::size_t>(std::ceil(rank));
    const double fraction = rank - static_cast<double>(lower);
    const double value = static_cast<double>(values[lower]) * (1.0 - fraction) +
        static_cast<double>(values[upper]) * fraction;
    return value / 1'000.0;
}

}  // namespace

void MetricsCollector::record(const IOCompletion& completion) {
    std::lock_guard lock(mutex_);
    auto& metrics = tenants_[completion.request.tenant_id];
    ++metrics.completed;
    metrics.bytes += static_cast<std::uint64_t>(completion.request.size_bytes);
    if (completion.status != IOStatus::Ok) {
        ++metrics.errors;
    }

    const auto queue_ns = std::max<std::int64_t>(0, completion.queueing_latency().count());
    const auto total_ns = std::max<std::int64_t>(0, completion.total_latency().count());
    metrics.queue_latency_ns.push_back(static_cast<std::uint64_t>(queue_ns));
    metrics.total_latency_ns.push_back(static_cast<std::uint64_t>(total_ns));
}

std::vector<TenantSummary> MetricsCollector::summarize(
    std::chrono::nanoseconds elapsed) const {
    const double elapsed_seconds = std::chrono::duration<double>(elapsed).count();
    std::lock_guard lock(mutex_);
    std::vector<TenantSummary> summaries;
    summaries.reserve(tenants_.size());

    for (const auto& [tenant_id, metrics] : tenants_) {
        const double safe_elapsed = elapsed_seconds > 0.0 ? elapsed_seconds : 1.0;
        summaries.push_back(TenantSummary{
            .tenant_id = tenant_id,
            .completed = metrics.completed,
            .errors = metrics.errors,
            .bytes = metrics.bytes,
            .operations_per_second = static_cast<double>(metrics.completed) / safe_elapsed,
            .mebibytes_per_second =
                static_cast<double>(metrics.bytes) / (1024.0 * 1024.0) / safe_elapsed,
            .queue_p50_us = percentile_us(metrics.queue_latency_ns, 0.50),
            .queue_p95_us = percentile_us(metrics.queue_latency_ns, 0.95),
            .queue_p99_us = percentile_us(metrics.queue_latency_ns, 0.99),
            .total_p50_us = percentile_us(metrics.total_latency_ns, 0.50),
            .total_p95_us = percentile_us(metrics.total_latency_ns, 0.95),
            .total_p99_us = percentile_us(metrics.total_latency_ns, 0.99),
        });
    }
    return summaries;
}

std::size_t MetricsCollector::completion_count() const {
    std::lock_guard lock(mutex_);
    std::size_t total = 0;
    for (const auto& [tenant_id, metrics] : tenants_) {
        static_cast<void>(tenant_id);
        total += static_cast<std::size_t>(metrics.completed);
    }
    return total;
}

}  // namespace ioshield

