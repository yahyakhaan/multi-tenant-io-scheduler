#include "ioshield/fifo_scheduler.hpp"

#include <utility>

namespace ioshield {

bool FifoScheduler::enqueue(IORequest request) {
    {
        std::lock_guard lock(mutex_);
        if (closed_) {
            return false;
        }
        queue_.push_back(std::move(request));
    }
    ready_.notify_one();
    return true;
}

std::optional<IORequest> FifoScheduler::wait_dequeue() {
    std::unique_lock lock(mutex_);
    ready_.wait(lock, [this] { return closed_ || !queue_.empty(); });

    if (queue_.empty()) {
        return std::nullopt;
    }

    IORequest request = std::move(queue_.front());
    queue_.pop_front();
    return request;
}

void FifoScheduler::close() {
    {
        std::lock_guard lock(mutex_);
        closed_ = true;
    }
    ready_.notify_all();
}

std::size_t FifoScheduler::queued() const {
    std::lock_guard lock(mutex_);
    return queue_.size();
}

}  // namespace ioshield

