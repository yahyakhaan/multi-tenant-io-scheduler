#include "ioshield/rate_limited_scheduler.hpp"
#include "ioshield/token_bucket.hpp"

#include <chrono>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

bool near(double actual, double expected, double tolerance) {
    return std::abs(actual - expected) <= tolerance;
}

ioshield::IORequest request(std::uint64_t id, std::string tenant, std::size_t bytes = 4096) {
    return ioshield::IORequest{
        .id = id,
        .tenant_id = std::move(tenant),
        .operation = ioshield::IOOperation::Read,
        .offset_bytes = id * 4096,
        .size_bytes = bytes,
        .submitted_at = ioshield::Clock::now(),
    };
}

void test_token_bucket_refill_and_burst_cap() {
    using namespace std::chrono_literals;
    const auto start = ioshield::Clock::time_point{};
    ioshield::TokenBucket bucket(10.0, 5.0, start);

    expect(bucket.try_consume(5.0, start), "initial burst is available");
    expect(!bucket.try_consume(1.0, start), "empty bucket rejects request");
    expect(near(bucket.available(start + 100ms), 1.0, 1e-9), "bucket refills from elapsed time");
    expect(bucket.try_consume(1.0, start + 100ms), "refilled token is consumable");
    expect(near(bucket.available(start + 10s), 5.0, 1e-9), "refill is capped at burst capacity");
}

void test_token_bucket_wait_time() {
    using namespace std::chrono_literals;
    const auto start = ioshield::Clock::time_point{};
    ioshield::TokenBucket bucket(20.0, 1.0, start);
    expect(bucket.try_consume(1.0, start), "single burst token consumed");
    expect(bucket.time_until_available(1.0, start) == 50ms, "wait time matches configured rate");
    expect(bucket.time_until_available(2.0, start) == std::chrono::nanoseconds::max(),
           "amount over capacity is permanently ineligible");
}

void test_dual_bucket_consumption_is_atomic() {
    using namespace std::chrono_literals;
    const auto start = ioshield::Clock::time_point{};
    ioshield::DualTokenBucket limiter(10.0, 2.0, 100.0, 50.0, start);

    expect(!limiter.try_consume(1.0, 60.0, start), "byte-ineligible request is rejected");
    expect(limiter.try_consume(2.0, 50.0, start),
           "failed byte check did not spend operation tokens");
    expect(!limiter.try_consume(1.0, 1.0, start), "operation bucket is now empty");
    expect(limiter.try_consume(1.0, 5.0, start + 100ms), "both resources refill together");
}

std::vector<ioshield::TenantRateLimit> basic_limits() {
    return {
        {"a", 20.0, 1.0, 4096.0 * 20.0, 4096.0},
        {"b", 1'000.0, 10.0, 4096.0 * 1'000.0, 4096.0 * 10.0},
    };
}

void test_scheduler_skips_ineligible_tenant() {
    ioshield::RateLimitedScheduler scheduler(basic_limits());
    expect(scheduler.enqueue(request(1, "a")), "first tenant-a request accepted");
    auto first = scheduler.wait_dequeue();
    expect(first && first->id == 1, "tenant-a consumes its initial burst");

    expect(scheduler.enqueue(request(2, "a")), "rate-limited request accepted into queue");
    expect(scheduler.enqueue(request(3, "b")), "eligible tenant-b request accepted");
    auto next = scheduler.wait_dequeue();
    expect(next && next->id == 3, "ineligible tenant does not block eligible tenant");

    scheduler.close();
    auto delayed = scheduler.wait_dequeue();
    expect(delayed && delayed->id == 2, "close drains queued rate-limited request");
    expect(!scheduler.wait_dequeue(), "closed drained scheduler returns no request");
}

void test_scheduler_enforces_rate() {
    using namespace std::chrono_literals;
    ioshield::RateLimitedScheduler scheduler({
        {"tenant", 100.0, 1.0, 4096.0 * 100.0, 4096.0},
    });

    for (std::uint64_t id = 0; id < 6; ++id) {
        expect(scheduler.enqueue(request(id, "tenant")), "rate test enqueue succeeds");
    }
    scheduler.close();

    const auto started = ioshield::Clock::now();
    std::vector<ioshield::Clock::time_point> dispatches;
    while (scheduler.wait_dequeue()) {
        dispatches.push_back(ioshield::Clock::now());
    }
    const auto elapsed = dispatches.back() - started;
    expect(dispatches.size() == 6, "rate test drains every request");
    expect(elapsed >= 45ms, "100 IOPS with burst one takes about 50 ms for six requests");
    expect(elapsed < 250ms, "rate limiter does not oversleep excessively");
}

void test_scheduler_rejects_invalid_requests() {
    ioshield::RateLimitedScheduler scheduler(basic_limits());
    expect(!scheduler.enqueue(request(1, "unknown")), "unknown tenant is rejected");
    expect(!scheduler.enqueue(request(2, "a", 8192)), "request larger than byte burst is rejected");
    scheduler.close();
    expect(!scheduler.enqueue(request(3, "a")), "closed scheduler rejects enqueue");
}

}  // namespace

int main() {
    test_token_bucket_refill_and_burst_cap();
    test_token_bucket_wait_time();
    test_dual_bucket_consumption_is_atomic();
    test_scheduler_skips_ineligible_tenant();
    test_scheduler_enforces_rate();
    test_scheduler_rejects_invalid_requests();

    if (failures != 0) {
        std::cerr << failures << " Phase 2 test assertion(s) failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "All Phase 2 tests passed\n";
    return EXIT_SUCCESS;
}

