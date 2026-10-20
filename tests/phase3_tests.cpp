#include "ioshield/drr_scheduler.hpp"

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
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

ioshield::DrrTenantConfig tenant(std::string id, std::uint32_t weight = 1) {
    constexpr double kGenerousRate = 1'000'000.0;
    constexpr double kGenerousBytes = 1024.0 * 1024.0 * 1024.0;
    return {
        .tenant_id = std::move(id),
        .weight = weight,
        .iops_per_second = kGenerousRate,
        .operation_burst = 10'000.0,
        .bytes_per_second = kGenerousBytes,
        .byte_burst = kGenerousBytes,
    };
}

void test_weighted_service_ratio() {
    ioshield::DrrScheduler scheduler({tenant("a", 1), tenant("b", 3)}, 4096);
    for (std::uint64_t id = 0; id < 200; ++id) {
        expect(scheduler.enqueue(request(id, "a")), "tenant-a enqueue succeeds");
        expect(scheduler.enqueue(request(10'000 + id, "b")), "tenant-b enqueue succeeds");
    }

    std::size_t served_a = 0;
    std::size_t served_b = 0;
    for (std::size_t i = 0; i < 120; ++i) {
        const auto next = scheduler.wait_dequeue();
        expect(next.has_value(), "weighted scheduler returns a request");
        if (next->tenant_id == "a") {
            ++served_a;
        } else {
            ++served_b;
        }
    }
    expect(served_a == 30 && served_b == 90, "weights 1:3 produce service ratio 1:3");
}

void test_variable_sizes_receive_equal_byte_share() {
    ioshield::DrrScheduler scheduler({tenant("small"), tenant("large")}, 4096);
    for (std::uint64_t id = 0; id < 500; ++id) {
        expect(scheduler.enqueue(request(id, "small", 4096)), "small request enqueued");
        expect(scheduler.enqueue(request(10'000 + id, "large", 16 * 1024)),
               "large request enqueued");
    }

    std::uint64_t small_bytes = 0;
    std::uint64_t large_bytes = 0;
    while (small_bytes + large_bytes < 2ULL * 1024ULL * 1024ULL) {
        const auto next = scheduler.wait_dequeue();
        expect(next.has_value(), "variable-size scheduler returns a request");
        if (next->tenant_id == "small") {
            small_bytes += next->size_bytes;
        } else {
            large_bytes += next->size_bytes;
        }
    }
    const double ratio = static_cast<double>(small_bytes) / static_cast<double>(large_bytes);
    expect(
        ratio >= 0.90 && ratio <= 1.10,
        "equal weights produce approximately equal byte shares; ratio=" +
            std::to_string(ratio));
}

void test_large_request_does_not_starve() {
    ioshield::DrrScheduler scheduler({tenant("large"), tenant("small")}, 4096);
    expect(scheduler.enqueue(request(1, "large", 64 * 1024)), "large request enqueued");
    for (std::uint64_t id = 0; id < 100; ++id) {
        expect(scheduler.enqueue(request(1'000 + id, "small", 4096)),
               "competing small request enqueued");
    }

    bool served_large = false;
    for (std::size_t i = 0; i < 32; ++i) {
        const auto next = scheduler.wait_dequeue();
        if (next->tenant_id == "large") {
            served_large = true;
            break;
        }
    }
    expect(served_large, "large request accumulates deficit and is not starved");
}

void test_rate_limited_tenant_does_not_block_peer() {
    using namespace std::chrono_literals;
    auto slow = tenant("slow");
    slow.iops_per_second = 5.0;
    slow.operation_burst = 1.0;
    slow.bytes_per_second = 4096.0 * 5.0;
    slow.byte_burst = 4096.0;

    ioshield::DrrScheduler scheduler({slow, tenant("fast")}, 4096);
    expect(scheduler.enqueue(request(1, "slow")), "first slow request enqueued");
    expect(scheduler.wait_dequeue()->id == 1, "slow tenant consumes initial burst");
    expect(scheduler.enqueue(request(2, "slow")), "second slow request queued");
    expect(scheduler.enqueue(request(3, "fast")), "fast request queued");

    const auto started = ioshield::Clock::now();
    const auto next = scheduler.wait_dequeue();
    expect(next && next->id == 3, "eligible peer bypasses rate-limited tenant");
    expect(ioshield::Clock::now() - started < 50ms, "peer dispatch does not wait for slow quota");
}

void test_idle_tenant_does_not_waste_capacity() {
    ioshield::DrrScheduler scheduler({tenant("idle", 10), tenant("active", 1)}, 4096);
    for (std::uint64_t id = 0; id < 20; ++id) {
        expect(scheduler.enqueue(request(id, "active")), "active-only request enqueued");
    }
    for (std::uint64_t id = 0; id < 20; ++id) {
        const auto next = scheduler.wait_dequeue();
        expect(next && next->tenant_id == "active", "active tenant uses idle capacity");
    }
}

void test_concurrent_producers_and_clean_shutdown() {
    ioshield::DrrScheduler scheduler(
        {tenant("a"), tenant("b"), tenant("c"), tenant("d")},
        4096);
    constexpr std::size_t kPerProducer = 250;
    std::vector<std::thread> producers;
    const std::vector<std::string> names{"a", "b", "c", "d"};
    std::atomic<std::size_t> enqueue_failures{0};
    std::atomic<std::size_t> consumed{0};

    std::thread consumer([&] {
        while (scheduler.wait_dequeue()) {
            ++consumed;
        }
    });

    for (std::size_t producer = 0; producer < names.size(); ++producer) {
        producers.emplace_back([&, producer] {
            for (std::size_t item = 0; item < kPerProducer; ++item) {
                const auto id = static_cast<std::uint64_t>(producer * kPerProducer + item);
                if (!scheduler.enqueue(request(id, names[producer]))) {
                    ++enqueue_failures;
                }
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    expect(enqueue_failures == 0, "all concurrent producer enqueues succeed");
    scheduler.close();
    consumer.join();
    expect(consumed == names.size() * kPerProducer, "concurrent producers lose no requests");
    expect(scheduler.queued() == 0, "clean shutdown drains all queues");
}

}  // namespace

int main() {
    test_weighted_service_ratio();
    test_variable_sizes_receive_equal_byte_share();
    test_large_request_does_not_starve();
    test_rate_limited_tenant_does_not_block_peer();
    test_idle_tenant_does_not_waste_capacity();
    test_concurrent_producers_and_clean_shutdown();

    if (failures != 0) {
        std::cerr << failures << " Phase 3 test assertion(s) failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "All Phase 3 tests passed\n";
    return EXIT_SUCCESS;
}

