#include "ioshield/fifo_scheduler.hpp"
#include "ioshield/metrics.hpp"
#include "ioshield/simulated_block_device.hpp"
#include "ioshield/workload.hpp"

#include <chrono>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

struct Options {
    double duration_seconds{3.0};
    double tenant_a_iops{6'000.0};
    double tenant_b_iops{500.0};
    double device_iops{5'000.0};
    double device_mib_per_sec{250.0};
    std::size_t lanes{1};
    std::uint64_t seed{42};
};

void print_usage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n"
        << "  --duration SECONDS\n"
        << "  --tenant-a-iops RATE\n"
        << "  --tenant-b-iops RATE\n"
        << "  --device-iops RATE\n"
        << "  --device-mibps RATE\n"
        << "  --lanes COUNT\n"
        << "  --seed VALUE\n";
}

Options parse_options(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string argument = argv[i];
        if (argument == "--help") {
            print_usage(argv[0]);
            std::exit(0);
        }
        if (i + 1 >= argc) {
            throw std::invalid_argument("missing value for " + argument);
        }
        const std::string value = argv[++i];
        if (argument == "--duration") {
            options.duration_seconds = std::stod(value);
        } else if (argument == "--tenant-a-iops") {
            options.tenant_a_iops = std::stod(value);
        } else if (argument == "--tenant-b-iops") {
            options.tenant_b_iops = std::stod(value);
        } else if (argument == "--device-iops") {
            options.device_iops = std::stod(value);
        } else if (argument == "--device-mibps") {
            options.device_mib_per_sec = std::stod(value);
        } else if (argument == "--lanes") {
            options.lanes = static_cast<std::size_t>(std::stoull(value));
        } else if (argument == "--seed") {
            options.seed = std::stoull(value);
        } else {
            throw std::invalid_argument("unknown option: " + argument);
        }
    }
    if (options.duration_seconds <= 0.0 || options.lanes == 0) {
        throw std::invalid_argument("duration and lane count must be positive");
    }
    return options;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace std::chrono_literals;
    using namespace ioshield;

    try {
        const Options options = parse_options(argc, argv);
        const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::duration<double>(options.duration_seconds));

        WorkloadConfig tenant_a{
            .tenant_id = "tenant-a-noisy",
            .offered_iops = options.tenant_a_iops,
            .duration = duration,
            .request_size_bytes = 4 * 1024,
            .read_ratio = 0.8,
            .arrival_pattern = ArrivalPattern::Fixed,
            .access_pattern = AccessPattern::Random,
            .seed = options.seed,
            .request_id_base = 1,
        };
        WorkloadConfig tenant_b{
            .tenant_id = "tenant-b-latency",
            .offered_iops = options.tenant_b_iops,
            .duration = duration,
            .request_size_bytes = 4 * 1024,
            .read_ratio = 1.0,
            .arrival_pattern = ArrivalPattern::Fixed,
            .access_pattern = AccessPattern::Random,
            .seed = options.seed + 1,
            .request_id_base = 1'000'000'000ULL,
        };

        auto workload = WorkloadGenerator::merge({
            WorkloadGenerator::generate(tenant_a),
            WorkloadGenerator::generate(tenant_b),
        });

        FifoScheduler scheduler;
        MetricsCollector metrics;
        SimulatedBlockDevice device(SimulatedDeviceConfig{
            .max_iops = options.device_iops,
            .max_bandwidth_mib_per_sec = options.device_mib_per_sec,
            .base_latency = 100us,
            .lanes = options.lanes,
            .queue_depth = options.lanes,
        });

        std::thread dispatcher([&] {
            while (auto request = scheduler.wait_dequeue()) {
                if (!device.submit(
                        std::move(*request),
                        [&](IOCompletion completion) { metrics.record(completion); })) {
                    break;
                }
            }
        });

        const auto started_at = Clock::now();
        for (auto& scheduled : workload) {
            std::this_thread::sleep_until(started_at + scheduled.scheduled_after);
            scheduled.request.submitted_at = started_at + scheduled.scheduled_after;
            if (!scheduler.enqueue(std::move(scheduled.request))) {
                throw std::runtime_error("scheduler closed while producing workload");
            }
        }

        scheduler.close();
        dispatcher.join();
        device.wait_idle();
        const auto finished_at = Clock::now();
        device.close();

        const auto elapsed = finished_at - started_at;
        std::cout << "scheduler,tenant,completed,errors,ops_per_sec,mib_per_sec,"
                     "queue_p50_us,queue_p95_us,queue_p99_us,total_p50_us,total_p95_us,total_p99_us\n";
        std::cout << std::fixed << std::setprecision(2);
        for (const auto& summary : metrics.summarize(elapsed)) {
            std::cout << "fifo," << summary.tenant_id << ',' << summary.completed << ','
                      << summary.errors << ',' << summary.operations_per_second << ','
                      << summary.mebibytes_per_second << ',' << summary.queue_p50_us << ','
                      << summary.queue_p95_us << ',' << summary.queue_p99_us << ','
                      << summary.total_p50_us << ',' << summary.total_p95_us << ','
                      << summary.total_p99_us << '\n';
        }

        if (metrics.completion_count() != workload.size()) {
            std::cerr << "incomplete run: expected " << workload.size() << " completions, got "
                      << metrics.completion_count() << '\n';
            return 2;
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }
}

