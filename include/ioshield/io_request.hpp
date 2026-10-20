#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

namespace ioshield {

using Clock = std::chrono::steady_clock;

enum class IOOperation : std::uint8_t {
    Read,
    Write,
};

enum class IOStatus : std::uint8_t {
    Ok,
    InvalidRequest,
    DeviceStopped,
};

struct IORequest {
    std::uint64_t id{};
    std::string tenant_id;
    IOOperation operation{IOOperation::Read};
    std::uint64_t offset_bytes{};
    std::size_t size_bytes{};
    Clock::time_point submitted_at{};
};

struct IOCompletion {
    IORequest request;
    IOStatus status{IOStatus::Ok};
    Clock::time_point dispatched_at{};
    Clock::time_point completed_at{};

    [[nodiscard]] std::chrono::nanoseconds queueing_latency() const {
        return dispatched_at - request.submitted_at;
    }

    [[nodiscard]] std::chrono::nanoseconds service_latency() const {
        return completed_at - dispatched_at;
    }

    [[nodiscard]] std::chrono::nanoseconds total_latency() const {
        return completed_at - request.submitted_at;
    }
};

}  // namespace ioshield

