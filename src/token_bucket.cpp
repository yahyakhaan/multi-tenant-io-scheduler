#include "ioshield/token_bucket.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

namespace ioshield {
namespace {

void validate_positive(double value, const char* name) {
    if (!std::isfinite(value) || value <= 0.0) {
        throw std::invalid_argument(std::string(name) + " must be finite and positive");
    }
}

}  // namespace

TokenBucket::TokenBucket(
    double rate_per_second,
    double capacity,
    Clock::time_point initial_time)
    : rate_per_second_(rate_per_second),
      capacity_(capacity),
      tokens_(capacity),
      last_refill_(initial_time) {
    validate_positive(rate_per_second_, "token rate");
    validate_positive(capacity_, "token capacity");
}

bool TokenBucket::can_consume(double amount, Clock::time_point now) {
    if (!std::isfinite(amount) || amount < 0.0) {
        throw std::invalid_argument("token amount must be finite and non-negative");
    }
    refill(now);
    return amount <= capacity_ && tokens_ + 1e-9 >= amount;
}

bool TokenBucket::try_consume(double amount, Clock::time_point now) {
    if (!can_consume(amount, now)) {
        return false;
    }
    tokens_ = std::max(0.0, tokens_ - amount);
    return true;
}

std::chrono::nanoseconds TokenBucket::time_until_available(
    double amount,
    Clock::time_point now) {
    if (!std::isfinite(amount) || amount < 0.0) {
        throw std::invalid_argument("token amount must be finite and non-negative");
    }
    refill(now);
    if (amount > capacity_) {
        return std::chrono::nanoseconds::max();
    }
    if (tokens_ + 1e-9 >= amount) {
        return std::chrono::nanoseconds::zero();
    }

    const double missing = amount - tokens_;
    const double nanoseconds = std::ceil((missing / rate_per_second_) * 1'000'000'000.0);
    if (nanoseconds >= static_cast<double>(std::numeric_limits<std::int64_t>::max())) {
        return std::chrono::nanoseconds::max();
    }
    return std::chrono::nanoseconds(static_cast<std::int64_t>(nanoseconds));
}

double TokenBucket::available(Clock::time_point now) {
    refill(now);
    return tokens_;
}

void TokenBucket::refill(Clock::time_point now) {
    if (now <= last_refill_) {
        return;
    }
    const double elapsed = std::chrono::duration<double>(now - last_refill_).count();
    tokens_ = std::min(capacity_, tokens_ + elapsed * rate_per_second_);
    last_refill_ = now;
}

DualTokenBucket::DualTokenBucket(
    double iops_per_second,
    double operation_burst,
    double bytes_per_second,
    double byte_burst,
    Clock::time_point initial_time)
    : operations_(iops_per_second, operation_burst, initial_time),
      bytes_(bytes_per_second, byte_burst, initial_time) {}

bool DualTokenBucket::can_consume(
    double operations,
    double bytes,
    Clock::time_point now) {
    return operations_.can_consume(operations, now) && bytes_.can_consume(bytes, now);
}

bool DualTokenBucket::try_consume(
    double operations,
    double bytes,
    Clock::time_point now) {
    if (!can_consume(operations, bytes, now)) {
        return false;
    }
    const bool operation_consumed = operations_.try_consume(operations, now);
    const bool bytes_consumed = bytes_.try_consume(bytes, now);
    if (!operation_consumed || !bytes_consumed) {
        throw std::logic_error("dual token bucket lost atomic eligibility");
    }
    return true;
}

std::chrono::nanoseconds DualTokenBucket::time_until_available(
    double operations,
    double bytes,
    Clock::time_point now) {
    return std::max(
        operations_.time_until_available(operations, now),
        bytes_.time_until_available(bytes, now));
}

double DualTokenBucket::operation_capacity() const noexcept {
    return operations_.capacity();
}

double DualTokenBucket::byte_capacity() const noexcept {
    return bytes_.capacity();
}

}  // namespace ioshield

