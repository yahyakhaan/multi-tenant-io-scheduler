#pragma once

#include "ioshield/scheduler.hpp"

#include <condition_variable>
#include <deque>
#include <mutex>

namespace ioshield {

class FifoScheduler final : public IScheduler {
public:
    bool enqueue(IORequest request) override;
    std::optional<IORequest> wait_dequeue() override;
    void close() override;
    [[nodiscard]] std::size_t queued() const override;

private:
    mutable std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<IORequest> queue_;
    bool closed_{false};
};

}  // namespace ioshield

