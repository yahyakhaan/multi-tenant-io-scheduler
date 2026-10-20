#pragma once

#include "ioshield/io_request.hpp"

#include <cstddef>
#include <optional>

namespace ioshield {

class IScheduler {
public:
    virtual ~IScheduler() = default;

    virtual bool enqueue(IORequest request) = 0;
    virtual std::optional<IORequest> wait_dequeue() = 0;
    virtual void close() = 0;
    [[nodiscard]] virtual std::size_t queued() const = 0;
};

}  // namespace ioshield

