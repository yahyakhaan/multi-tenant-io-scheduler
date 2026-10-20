#pragma once

#include "ioshield/io_request.hpp"

#include <chrono>

namespace ioshield {

class TokenBucket {
public:
    TokenBucket(
        double rate_per_second,
        double capacity,
        Clock::time_point initial_time = Clock::now());

    [[nodiscard]] bool can_consume(double amount, Clock::time_point now);
    bool try_consume(double amount, Clock::time_point now);
    [[nodiscard]] std::chrono::nanoseconds time_until_available(
        double amount,
        Clock::time_point now);
    [[nodiscard]] double available(Clock::time_point now);
    [[nodiscard]] double capacity() const noexcept { return capacity_; }

private:
    void refill(Clock::time_point now);

    double rate_per_second_;
    double capacity_;
    double tokens_;
    Clock::time_point last_refill_;
};

class DualTokenBucket {
public:
    DualTokenBucket(
        double iops_per_second,
        double operation_burst,
        double bytes_per_second,
        double byte_burst,
        Clock::time_point initial_time = Clock::now());

    bool try_consume(double operations, double bytes, Clock::time_point now);
    [[nodiscard]] std::chrono::nanoseconds time_until_available(
        double operations,
        double bytes,
        Clock::time_point now);
    [[nodiscard]] double operation_capacity() const noexcept;
    [[nodiscard]] double byte_capacity() const noexcept;

private:
    TokenBucket operations_;
    TokenBucket bytes_;
};

}  // namespace ioshield

