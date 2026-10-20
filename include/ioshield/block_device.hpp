#pragma once

#include "ioshield/io_request.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>

namespace ioshield {

using CompletionCallback = std::function<void(IOCompletion)>;

struct DeviceStats {
    std::uint64_t submitted{};
    std::uint64_t completed{};
    std::uint64_t errors{};
    std::uint64_t busy_time_ns{};
    std::size_t in_flight{};
    std::size_t lanes{};
};

class IBlockDevice {
public:
    virtual ~IBlockDevice() = default;

    // Blocks while the device submission queue is full. Returns false after close().
    virtual bool submit(IORequest request, CompletionCallback completion) = 0;
    virtual void wait_idle() = 0;
    virtual void close() = 0;
    [[nodiscard]] virtual DeviceStats stats() const = 0;
};

}  // namespace ioshield

