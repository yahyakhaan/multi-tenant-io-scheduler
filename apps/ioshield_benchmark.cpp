#include "ioshield/drr_scheduler.hpp"
#include "ioshield/fifo_scheduler.hpp"
#include "ioshield/metrics.hpp"
#include "ioshield/rate_limited_scheduler.hpp"
#include "ioshield/simulated_block_device.hpp"
#include "ioshield/workload.hpp"

#include <chrono>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

struct Options {
    std::string scheduler{"fifo"};
    double duration_seconds{3.0};
    double tenant_a_iops{6'000.0};
    double tenant_b_iops{500.0};
    double device_iops{5'000.0};
    double device_mib_per_sec{250.0};
    double tenant_a_limit_iops{3'000.0};
    double tenant_b_limit_iops{500.0};
    double tenant_a_limit_mib_per_sec{12.0};
    double tenant_b_limit_mib_per_sec{2.0};
    double burst_requests{32.0};
    std::uint32_t tenant_a_weight{1};
    std::uint32_t tenant_b_weight{1};
    std::size_t drr_quantum_bytes{4096};
    std::size_t lanes{1};
    std::uint64_t seed{42};
};

void print_usage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n"
        << "  --scheduler fifo|token-bucket|drr\n"
        << "  --duration SECONDS\n"
        << "  --tenant-a-iops RATE\n"
        << "  --tenant-b-iops RATE\n"
        << "  --device-iops RATE\n"
        << "  --device-mibps RATE\n"
        << "  --tenant-a-limit-iops RATE\n"
        << "  --tenant-b-limit-iops RATE\n"
        << "  --tenant-a-limit-mibps RATE\n"
        << "  --tenant-b-limit-mibps RATE\n"
        << "  --burst-requests COUNT\n"
        << "  --tenant-a-weight WEIGHT\n"
        << "  --tenant-b-weight WEIGHT\n"
        << "  --drr-quantum-bytes BYTES\n"
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
        if (argument == "--scheduler") {
            options.scheduler = value;
        } else if (argument == "--duration") {
            options.duration_seconds = std::stod(value);
        } else if (argument == "--tenant-a-iops") {
            options.tenant_a_iops = std::stod(value);
        } else if (argument == "--tenant-b-iops") {
            options.tenant_b_iops = std::stod(value);
        } else if (argument == "--device-iops") {
            options.device_iops = std::stod(value);
        } else if (argument == "--device-mibps") {
            options.device_mib_per_sec = std::stod(value);
        } else if (argument == "--tenant-a-limit-iops") {
            options.tenant_a_limit_iops = std::stod(value);
        } else if (argument == "--tenant-b-limit-iops") {
            options.tenant_b_limit_iops = std::stod(value);
        } else if (argument == "--tenant-a-limit-mibps") {
            options.tenant_a_limit_mib_per_sec = std::stod(value);
        } else if (argument == "--tenant-b-limit-mibps") {
            options.tenant_b_limit_mib_per_sec = std::stod(value);
        } else if (argument == "--burst-requests") {
            options.burst_requests = std::stod(value);
        } else if (argument == "--tenant-a-weight") {
            options.tenant_a_weight = static_cast<std::uint32_t>(std::stoul(value));
        } else if (argument == "--tenant-b-weight") {
            options.tenant_b_weight = static_cast<std::uint32_t>(std::stoul(value));
        } else if (argument == "--drr-quantum-bytes") {
            options.drr_quantum_bytes = static_cast<std::size_t>(std::stoull(value));
        } else if (argument == "--lanes") {
            options.lanes = static_cast<std::size_t>(std::stoull(value));
        } else if (argument == "--seed") {
            options.seed = std::stoull(value);
        } else {
            throw std::invalid_argument("unknown option: " + argument);
        }
    }
    const bool scheduler_valid = options.scheduler == "fifo" ||
        options.scheduler == "token-bucket" || options.scheduler == "drr";
    const bool limits_valid = options.tenant_a_limit_iops > 0.0 &&
        options.tenant_b_limit_iops > 0.0 &&
        options.tenant_a_limit_mib_per_sec > 0.0 &&
        options.tenant_b_limit_mib_per_sec > 0.0 && options.burst_requests >= 1.0 &&
        options.tenant_a_weight > 0 && options.tenant_b_weight > 0 &&
        options.drr_quantum_bytes > 0;
    if (!scheduler_valid || options.duration_seconds <= 0.0 || options.lanes == 0 ||
        !limits_valid) {
        throw std::invalid_argument("invalid scheduler or non-positive benchmark setting");
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

        constexpr double kBytesPerMebibyte = 1024.0 * 1024.0;
        constexpr double kRequestBytes = 4.0 * 1024.0;
        std::unique_ptr<IScheduler> scheduler;
        if (options.scheduler == "fifo") {
            scheduler = std::make_unique<FifoScheduler>();
        } else if (options.scheduler == "token-bucket") {
            scheduler = std::make_unique<RateLimitedScheduler>(
                std::vector<TenantRateLimit>{
                    {
                        "tenant-a-noisy",
                        options.tenant_a_limit_iops,
                        options.burst_requests,
                        options.tenant_a_limit_mib_per_sec * kBytesPerMebibyte,
                        options.burst_requests * kRequestBytes,
                    },
                    {
                        "tenant-b-latency",
                        options.tenant_b_limit_iops,
                        options.burst_requests,
                        options.tenant_b_limit_mib_per_sec * kBytesPerMebibyte,
                        options.burst_requests * kRequestBytes,
                    },
                });
        } else {
            scheduler = std::make_unique<DrrScheduler>(
                std::vector<DrrTenantConfig>{
                    {
                        "tenant-a-noisy",
                        options.tenant_a_weight,
                        options.tenant_a_limit_iops,
                        options.burst_requests,
                        options.tenant_a_limit_mib_per_sec * kBytesPerMebibyte,
                        options.burst_requests * kRequestBytes,
                    },
                    {
                        "tenant-b-latency",
                        options.tenant_b_weight,
                        options.tenant_b_limit_iops,
                        options.burst_requests,
                        options.tenant_b_limit_mib_per_sec * kBytesPerMebibyte,
                        options.burst_requests * kRequestBytes,
                    },
                },
                options.drr_quantum_bytes);
        }

        MetricsCollector metrics;
        SimulatedBlockDevice device(SimulatedDeviceConfig{
            .max_iops = options.device_iops,
            .max_bandwidth_mib_per_sec = options.device_mib_per_sec,
            .base_latency = 100us,
            .lanes = options.lanes,
            .queue_depth = options.lanes,
        });

        std::thread dispatcher([&] {
            while (auto request = scheduler->wait_dequeue()) {
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
            if (!scheduler->enqueue(std::move(scheduled.request))) {
                throw std::runtime_error("scheduler closed while producing workload");
            }
        }

        const auto measurement_elapsed = Clock::now() - started_at;
        const auto measurement_summaries = metrics.summarize(measurement_elapsed);
        scheduler->close();
        dispatcher.join();
        device.wait_idle();
        device.close();

        std::cout << "scheduler,tenant,completed,errors,ops_per_sec,mib_per_sec,"
                     "queue_p50_us,queue_p95_us,queue_p99_us,total_p50_us,total_p95_us,total_p99_us\n";
        std::cout << std::fixed << std::setprecision(2);
        for (const auto& summary : measurement_summaries) {
            std::cout << options.scheduler << ',' << summary.tenant_id << ',' << summary.completed << ','
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

