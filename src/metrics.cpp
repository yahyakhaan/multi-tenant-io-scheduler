#include "ioshield/metrics.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

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

void MetricsCollector::record_submission(const IORequest& request) {
    std::lock_guard lock(mutex_);
    ++tenants_[request.tenant_id].submitted;
}

void MetricsCollector::record_rejection(const IORequest& request) {
    std::lock_guard lock(mutex_);
    ++tenants_[request.tenant_id].rejected;
}

void MetricsCollector::record(const IOCompletion& completion) {
    std::lock_guard lock(mutex_);
    auto& metrics = tenants_[completion.request.tenant_id];
    ++metrics.completed;
    metrics.bytes += static_cast<std::uint64_t>(completion.request.size_bytes);
    if (completion.status != IOStatus::Ok) {
        ++metrics.errors;
    }

    const auto queue_ns = std::max<std::int64_t>(0, completion.queueing_latency().count());
    const auto service_ns = std::max<std::int64_t>(0, completion.service_latency().count());
    const auto total_ns = std::max<std::int64_t>(0, completion.total_latency().count());
    metrics.queue_latency_ns.push_back(static_cast<std::uint64_t>(queue_ns));
    metrics.service_latency_ns.push_back(static_cast<std::uint64_t>(service_ns));
    metrics.total_latency_ns.push_back(static_cast<std::uint64_t>(total_ns));
    metrics.completions.push_back(TenantMetrics::CompletionSample{
        .completed_at = completion.completed_at,
        .bytes = static_cast<std::uint64_t>(completion.request.size_bytes),
        .error = completion.status != IOStatus::Ok,
        .total_latency_ns = static_cast<std::uint64_t>(total_ns),
    });
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
            .submitted = metrics.submitted,
            .rejected = metrics.rejected,
            .completed = metrics.completed,
            .errors = metrics.errors,
            .bytes = metrics.bytes,
            .operations_per_second = static_cast<double>(metrics.completed) / safe_elapsed,
            .mebibytes_per_second =
                static_cast<double>(metrics.bytes) / (1024.0 * 1024.0) / safe_elapsed,
            .queue_p50_us = percentile_us(metrics.queue_latency_ns, 0.50),
            .queue_p95_us = percentile_us(metrics.queue_latency_ns, 0.95),
            .queue_p99_us = percentile_us(metrics.queue_latency_ns, 0.99),
            .service_p50_us = percentile_us(metrics.service_latency_ns, 0.50),
            .service_p95_us = percentile_us(metrics.service_latency_ns, 0.95),
            .service_p99_us = percentile_us(metrics.service_latency_ns, 0.99),
            .total_p50_us = percentile_us(metrics.total_latency_ns, 0.50),
            .total_p95_us = percentile_us(metrics.total_latency_ns, 0.95),
            .total_p99_us = percentile_us(metrics.total_latency_ns, 0.99),
        });
    }
    return summaries;
}

std::vector<TimeSeriesPoint> MetricsCollector::summarize_windows(
    Clock::time_point start,
    Clock::time_point end,
    std::chrono::nanoseconds window) const {
    if (end <= start || window <= std::chrono::nanoseconds::zero()) {
        throw std::invalid_argument("metrics time-series range and window must be positive");
    }

    const auto elapsed = end - start;
    const auto window_count = static_cast<std::size_t>(
        (elapsed + window - std::chrono::nanoseconds(1)) / window);
    std::lock_guard lock(mutex_);
    std::vector<TimeSeriesPoint> points;
    points.reserve(window_count * tenants_.size());

    for (const auto& [tenant_id, metrics] : tenants_) {
        for (std::size_t index = 0; index < window_count; ++index) {
            const auto window_start = start + window * static_cast<std::int64_t>(index);
            const auto window_end = std::min(end, window_start + window);
            std::uint64_t completed = 0;
            std::uint64_t errors = 0;
            std::uint64_t bytes = 0;
            std::vector<std::uint64_t> total_latencies;

            for (const auto& sample : metrics.completions) {
                if (sample.completed_at < window_start || sample.completed_at >= window_end) {
                    continue;
                }
                ++completed;
                errors += sample.error ? 1U : 0U;
                bytes += sample.bytes;
                total_latencies.push_back(sample.total_latency_ns);
            }

            const double seconds = std::chrono::duration<double>(window_end - window_start).count();
            points.push_back(TimeSeriesPoint{
                .window_index = index,
                .tenant_id = tenant_id,
                .completed = completed,
                .errors = errors,
                .bytes = bytes,
                .operations_per_second = static_cast<double>(completed) / seconds,
                .mebibytes_per_second =
                    static_cast<double>(bytes) / (1024.0 * 1024.0) / seconds,
                .total_p95_us = percentile_us(total_latencies, 0.95),
                .total_p99_us = percentile_us(total_latencies, 0.99),
            });
        }
    }
    return points;
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

double jains_fairness(const std::vector<double>& allocations) {
    if (allocations.empty()) {
        return 1.0;
    }
    double sum = 0.0;
    double squared_sum = 0.0;
    for (const double allocation : allocations) {
        if (!std::isfinite(allocation) || allocation < 0.0) {
            throw std::invalid_argument("fairness allocations must be finite and non-negative");
        }
        sum += allocation;
        squared_sum += allocation * allocation;
    }
    if (squared_sum == 0.0) {
        return 1.0;
    }
    return (sum * sum) / (static_cast<double>(allocations.size()) * squared_sum);
}

}  // namespace ioshield

