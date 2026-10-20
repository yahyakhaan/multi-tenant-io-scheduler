#include "ioshield/fifo_scheduler.hpp"
#include "ioshield/metrics.hpp"
#include "ioshield/simulated_block_device.hpp"
#include "ioshield/workload.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <mutex>
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

ioshield::IORequest request(std::uint64_t id, std::string tenant = "tenant") {
    return ioshield::IORequest{
        .id = id,
        .tenant_id = std::move(tenant),
        .operation = ioshield::IOOperation::Read,
        .offset_bytes = id * 4096,
        .size_bytes = 4096,
        .submitted_at = ioshield::Clock::now(),
    };
}

void test_fifo_order_and_close() {
    ioshield::FifoScheduler scheduler;
    expect(scheduler.enqueue(request(1)), "FIFO accepts first request");
    expect(scheduler.enqueue(request(2)), "FIFO accepts second request");
    expect(scheduler.queued() == 2, "FIFO reports queue length");

    const auto first = scheduler.wait_dequeue();
    const auto second = scheduler.wait_dequeue();
    expect(first && first->id == 1, "FIFO preserves first request");
    expect(second && second->id == 2, "FIFO preserves second request");

    scheduler.close();
    expect(!scheduler.enqueue(request(3)), "closed FIFO rejects enqueue");
    expect(!scheduler.wait_dequeue(), "closed empty FIFO returns no request");
}

void test_workload_is_deterministic() {
    using namespace std::chrono_literals;
    const ioshield::WorkloadConfig config{
        .tenant_id = "a",
        .offered_iops = 1'000.0,
        .duration = 10ms,
        .request_size_bytes = 4096,
        .working_set_bytes = 4096 * 32,
        .read_ratio = 0.75,
        .arrival_pattern = ioshield::ArrivalPattern::Poisson,
        .access_pattern = ioshield::AccessPattern::Random,
        .seed = 99,
    };
    const auto first = ioshield::WorkloadGenerator::generate(config);
    const auto second = ioshield::WorkloadGenerator::generate(config);
    expect(!first.empty(), "workload generator emits requests");
    expect(first.size() == second.size(), "same seed emits same request count");
    for (std::size_t i = 0; i < first.size() && i < second.size(); ++i) {
        expect(first[i].scheduled_after == second[i].scheduled_after,
               "same seed emits same arrival times");
        expect(first[i].request.offset_bytes == second[i].request.offset_bytes,
               "same seed emits same offsets");
        expect(first[i].request.operation == second[i].request.operation,
               "same seed emits same operations");
    }
}

void test_merge_order() {
    using namespace std::chrono_literals;
    std::vector<ioshield::ScheduledRequest> a{
        {2ms, request(2, "a")},
        {4ms, request(4, "a")},
    };
    std::vector<ioshield::ScheduledRequest> b{
        {1ms, request(1, "b")},
        {3ms, request(3, "b")},
    };
    const auto merged = ioshield::WorkloadGenerator::merge({a, b});
    expect(merged.size() == 4, "merge preserves all requests");
    for (std::size_t i = 0; i < merged.size(); ++i) {
        expect(merged[i].request.id == i + 1, "merge sorts by arrival time");
    }
}

void test_simulated_device_completes() {
    using namespace std::chrono_literals;
    ioshield::SimulatedBlockDevice device(ioshield::SimulatedDeviceConfig{
        .max_iops = 100'000.0,
        .max_bandwidth_mib_per_sec = 1'000.0,
        .base_latency = 10us,
        .lanes = 1,
        .queue_depth = 1,
    });

    std::mutex mutex;
    std::condition_variable completed;
    bool done = false;
    auto item = request(7);
    expect(device.submit(
               std::move(item), [&](ioshield::IOCompletion result) {
                   std::lock_guard lock(mutex);
                   done = result.request.id == 7 && result.status == ioshield::IOStatus::Ok;
                   completed.notify_one();
               }),
           "simulated device accepts request");

    {
        std::unique_lock lock(mutex);
        completed.wait_for(lock, 1s, [&] { return done; });
    }
    expect(done, "simulated device invokes completion");
    device.wait_idle();
    device.close();
}

void test_metrics_percentiles() {
    using namespace std::chrono_literals;
    ioshield::MetricsCollector metrics;
    const auto start = ioshield::Clock::now();
    for (std::uint64_t i = 1; i <= 5; ++i) {
        auto item = request(i, "metrics-tenant");
        item.submitted_at = start;
        metrics.record(ioshield::IOCompletion{
            .request = std::move(item),
            .status = ioshield::IOStatus::Ok,
            .dispatched_at = start + std::chrono::microseconds(i),
            .completed_at = start + std::chrono::microseconds(i * 10),
        });
    }
    const auto summaries = metrics.summarize(1s);
    expect(summaries.size() == 1, "metrics emits one tenant summary");
    expect(summaries[0].completed == 5, "metrics counts completions");
    expect(summaries[0].total_p50_us == 30.0, "metrics computes median latency");
    expect(summaries[0].operations_per_second == 5.0, "metrics computes throughput");
}

}  // namespace

int main() {
    std::cout << "test_fifo_order_and_close\n";
    test_fifo_order_and_close();
    std::cout << "test_workload_is_deterministic\n";
    test_workload_is_deterministic();
    std::cout << "test_merge_order\n";
    test_merge_order();
    std::cout << "test_simulated_device_completes\n";
    test_simulated_device_completes();
    std::cout << "test_metrics_percentiles\n";
    test_metrics_percentiles();

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "All Phase 1 tests passed\n";
    return EXIT_SUCCESS;
}

