#pragma once

#include "ioshield/io_request.hpp"

#include <functional>

namespace ioshield {

using CompletionCallback = std::function<void(IOCompletion)>;

class IBlockDevice {
public:
    virtual ~IBlockDevice() = default;

    // Blocks while the device submission queue is full. Returns false after close().
    virtual bool submit(IORequest request, CompletionCallback completion) = 0;
    virtual void wait_idle() = 0;
    virtual void close() = 0;
};

}  // namespace ioshield

