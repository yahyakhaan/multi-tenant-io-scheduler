#pragma once

#include "ioshield/io_request.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace ioshield {

enum class ArrivalPattern : std::uint8_t {
    Fixed,
    Poisson,
};

enum class AccessPattern : std::uint8_t {
    Random,
    Sequential,
};

struct WorkloadConfig {
    std::string tenant_id;
    double offered_iops{1'000.0};
    std::chrono::nanoseconds duration{std::chrono::seconds(1)};
    std::chrono::nanoseconds start_after{};
    std::size_t request_size_bytes{4 * 1024};
    std::uint64_t working_set_bytes{1ULL << 30};
    double read_ratio{0.5};
    ArrivalPattern arrival_pattern{ArrivalPattern::Fixed};
    AccessPattern access_pattern{AccessPattern::Random};
    std::uint64_t seed{1};
    std::uint64_t request_id_base{};
};

struct ScheduledRequest {
    std::chrono::nanoseconds scheduled_after{};
    IORequest request;
};

struct WorkloadPhase {
    std::chrono::nanoseconds start_after{};
    std::chrono::nanoseconds duration{std::chrono::seconds(1)};
    double offered_iops{1'000.0};
};

class WorkloadGenerator {
public:
    [[nodiscard]] static std::vector<ScheduledRequest> generate(const WorkloadConfig& config);
    [[nodiscard]] static std::vector<ScheduledRequest> generate_phased(
        const WorkloadConfig& base_config,
        const std::vector<WorkloadPhase>& phases);
    [[nodiscard]] static std::vector<ScheduledRequest> merge(
        std::vector<std::vector<ScheduledRequest>> workloads);
};

}  // namespace ioshield

