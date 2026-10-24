#include "ioshield/workload.hpp"

#include <algorithm>
#include <cmath>
#include <iterator>
#include <limits>
#include <random>
#include <stdexcept>

namespace ioshield {
namespace {

void validate(const WorkloadConfig& config) {
    if (config.tenant_id.empty()) {
        throw std::invalid_argument("tenant ID must not be empty");
    }
    if (config.offered_iops <= 0.0) {
        throw std::invalid_argument("offered IOPS must be positive");
    }
    if (config.duration <= std::chrono::nanoseconds::zero()) {
        throw std::invalid_argument("duration must be positive");
    }
    if (config.request_size_bytes == 0 || config.working_set_bytes < config.request_size_bytes) {
        throw std::invalid_argument("invalid request size or working set");
    }
    if (config.read_ratio < 0.0 || config.read_ratio > 1.0) {
        throw std::invalid_argument("read ratio must be between zero and one");
    }
}

}  // namespace

std::vector<ScheduledRequest> WorkloadGenerator::generate(const WorkloadConfig& config) {
    validate(config);

    std::mt19937_64 random(config.seed);
    std::bernoulli_distribution choose_read(config.read_ratio);
    const std::uint64_t block_count =
        config.working_set_bytes / static_cast<std::uint64_t>(config.request_size_bytes);
    std::uniform_int_distribution<std::uint64_t> choose_block(0, block_count - 1);
    std::exponential_distribution<double> poisson_gap(config.offered_iops);

    const auto fixed_gap = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::duration<double>(1.0 / config.offered_iops));
    if (fixed_gap <= std::chrono::nanoseconds::zero()) {
        throw std::invalid_argument("offered IOPS exceeds nanosecond timer resolution");
    }
    std::chrono::nanoseconds elapsed{};
    std::uint64_t sequence = 0;
    std::vector<ScheduledRequest> requests;

    const double estimated_count =
        std::chrono::duration<double>(config.duration).count() * config.offered_iops;
    if (estimated_count < static_cast<double>(std::numeric_limits<std::size_t>::max())) {
        requests.reserve(static_cast<std::size_t>(std::ceil(estimated_count)));
    }

    while (elapsed < config.duration) {
        const std::uint64_t block = config.access_pattern == AccessPattern::Random
            ? choose_block(random)
            : sequence % block_count;
        requests.push_back(ScheduledRequest{
            .scheduled_after = config.start_after + elapsed,
            .request = IORequest{
                .id = config.request_id_base + sequence,
                .tenant_id = config.tenant_id,
                .operation = choose_read(random) ? IOOperation::Read : IOOperation::Write,
                .offset_bytes = block * static_cast<std::uint64_t>(config.request_size_bytes),
                .size_bytes = config.request_size_bytes,
            },
        });
        ++sequence;

        if (config.arrival_pattern == ArrivalPattern::Fixed) {
            elapsed += fixed_gap;
        } else {
            elapsed += std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::duration<double>(poisson_gap(random)));
        }
    }

    return requests;
}

std::vector<ScheduledRequest> WorkloadGenerator::generate_phased(
    const WorkloadConfig& base_config,
    const std::vector<WorkloadPhase>& phases) {
    if (phases.empty()) {
        throw std::invalid_argument("phased workload requires at least one phase");
    }

    std::vector<std::vector<ScheduledRequest>> generated;
    generated.reserve(phases.size());
    std::uint64_t next_request_id = base_config.request_id_base;

    for (std::size_t index = 0; index < phases.size(); ++index) {
        const auto& phase = phases[index];
        if (phase.start_after < std::chrono::nanoseconds::zero() ||
            phase.duration <= std::chrono::nanoseconds::zero() ||
            !std::isfinite(phase.offered_iops) || phase.offered_iops <= 0.0) {
            throw std::invalid_argument("workload phases require non-negative starts and positive rates");
        }

        WorkloadConfig phase_config = base_config;
        phase_config.start_after = base_config.start_after + phase.start_after;
        phase_config.duration = phase.duration;
        phase_config.offered_iops = phase.offered_iops;
        phase_config.seed = base_config.seed + static_cast<std::uint64_t>(index);
        phase_config.request_id_base = next_request_id;

        auto requests = generate(phase_config);
        if (requests.size() >
            std::numeric_limits<std::uint64_t>::max() - next_request_id) {
            throw std::overflow_error("phased workload request IDs overflow 64 bits");
        }
        next_request_id += static_cast<std::uint64_t>(requests.size());
        generated.push_back(std::move(requests));
    }
    return merge(std::move(generated));
}

std::vector<ScheduledRequest> WorkloadGenerator::merge(
    std::vector<std::vector<ScheduledRequest>> workloads) {
    std::size_t total = 0;
    for (const auto& workload : workloads) {
        total += workload.size();
    }

    std::vector<ScheduledRequest> merged;
    merged.reserve(total);
    for (auto& workload : workloads) {
        std::move(workload.begin(), workload.end(), std::back_inserter(merged));
    }

    std::stable_sort(merged.begin(), merged.end(), [](const auto& left, const auto& right) {
        if (left.scheduled_after != right.scheduled_after) {
            return left.scheduled_after < right.scheduled_after;
        }
        if (left.request.tenant_id != right.request.tenant_id) {
            return left.request.tenant_id < right.request.tenant_id;
        }
        return left.request.id < right.request.id;
    });
    return merged;
}

}  // namespace ioshield

