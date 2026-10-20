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
    std::uint64_t completed{};
    std::uint64_t errors{};
    std::uint64_t bytes{};
    double operations_per_second{};
    double mebibytes_per_second{};
    double queue_p50_us{};
    double queue_p95_us{};
    double queue_p99_us{};
    double total_p50_us{};
    double total_p95_us{};
    double total_p99_us{};
};

class MetricsCollector {
public:
    void record(const IOCompletion& completion);

    [[nodiscard]] std::vector<TenantSummary> summarize(
        std::chrono::nanoseconds elapsed) const;
    [[nodiscard]] std::size_t completion_count() const;

private:
    struct TenantMetrics {
        std::uint64_t completed{};
        std::uint64_t errors{};
        std::uint64_t bytes{};
        std::vector<std::uint64_t> queue_latency_ns;
        std::vector<std::uint64_t> total_latency_ns;
    };

    mutable std::mutex mutex_;
    std::map<std::string, TenantMetrics> tenants_;
};

}  // namespace ioshield

