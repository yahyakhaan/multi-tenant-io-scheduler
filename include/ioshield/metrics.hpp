#pragma once

#include "ioshield/io_request.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace ioshield {

struct TenantSummary {
    std::string tenant_id;
    std::uint64_t submitted{};
    std::uint64_t rejected{};
    std::uint64_t completed{};
    std::uint64_t errors{};
    std::uint64_t bytes{};
    double operations_per_second{};
    double mebibytes_per_second{};
    double queue_p50_us{};
    double queue_p95_us{};
    double queue_p99_us{};
    double service_p50_us{};
    double service_p95_us{};
    double service_p99_us{};
    double total_p50_us{};
    double total_p95_us{};
    double total_p99_us{};
};

struct TimeSeriesPoint {
    std::size_t window_index{};
    std::string tenant_id;
    std::uint64_t completed{};
    std::uint64_t errors{};
    std::uint64_t bytes{};
    double operations_per_second{};
    double mebibytes_per_second{};
    double total_p95_us{};
    double total_p99_us{};
};

class MetricsCollector {
public:
    void record_submission(const IORequest& request);
    void record_rejection(const IORequest& request);
    void record(const IOCompletion& completion);

    [[nodiscard]] std::vector<TenantSummary> summarize(
        std::chrono::nanoseconds elapsed) const;
    [[nodiscard]] std::vector<TimeSeriesPoint> summarize_windows(
        Clock::time_point start,
        Clock::time_point end,
        std::chrono::nanoseconds window) const;
    [[nodiscard]] std::size_t completion_count() const;

private:
    struct TenantMetrics {
        struct CompletionSample {
            Clock::time_point completed_at;
            std::uint64_t bytes{};
            bool error{};
            std::uint64_t total_latency_ns{};
        };

        std::uint64_t submitted{};
        std::uint64_t rejected{};
        std::uint64_t completed{};
        std::uint64_t errors{};
        std::uint64_t bytes{};
        std::vector<std::uint64_t> queue_latency_ns;
        std::vector<std::uint64_t> service_latency_ns;
        std::vector<std::uint64_t> total_latency_ns;
        std::vector<CompletionSample> completions;
    };

    mutable std::mutex mutex_;
    std::map<std::string, TenantMetrics> tenants_;
};

[[nodiscard]] double jains_fairness(const std::vector<double>& allocations);

}  // namespace ioshield

