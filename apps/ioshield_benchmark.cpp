#include "ioshield/drr_scheduler.hpp"
#include "ioshield/file_block_device.hpp"
#include "ioshield/fifo_scheduler.hpp"
#include "ioshield/metrics.hpp"
#include "ioshield/rate_limited_scheduler.hpp"
#include "ioshield/simulated_block_device.hpp"
#include "ioshield/workload.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

struct Options {
    std::string scheduler{"fifo"};
    std::string device{"simulated"};
    std::string file_path{"ioshield-benchmark.dat"};
    std::string file_durability{"buffered"};
    std::string time_series_path;
    std::uint64_t file_size_mib{1024};
    double duration_seconds{3.0};
    double tenant_a_iops{6'000.0};
    double tenant_b_iops{500.0};
    double tenant_a_spike_after_seconds{-1.0};
    double tenant_a_spike_multiplier{2.0};
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
    std::size_t tenant_a_request_kib{4};
    std::size_t tenant_b_request_kib{4};
    std::string tenant_a_access{"random"};
    std::string tenant_b_access{"random"};
    std::size_t lanes{1};
    std::uint64_t seed{42};
};

void print_usage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n"
        << "  --scheduler fifo|token-bucket|drr\n"
        << "  --device simulated|file\n"
        << "  --file-path PATH\n"
        << "  --file-size-mib SIZE\n"
        << "  --file-durability buffered|fsync\n"
        << "  --timeseries PATH\n"
        << "  --duration SECONDS\n"
        << "  --tenant-a-iops RATE\n"
        << "  --tenant-b-iops RATE\n"
        << "  --tenant-a-spike-after SECONDS (disabled by default)\n"
        << "  --tenant-a-spike-multiplier FACTOR\n"
        << "  --tenant-a-request-kib SIZE\n"
        << "  --tenant-b-request-kib SIZE\n"
        << "  --tenant-a-access random|sequential\n"
        << "  --tenant-b-access random|sequential\n"
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
        } else if (argument == "--device") {
            options.device = value;
        } else if (argument == "--file-path") {
            options.file_path = value;
        } else if (argument == "--file-size-mib") {
            options.file_size_mib = std::stoull(value);
        } else if (argument == "--file-durability") {
            options.file_durability = value;
        } else if (argument == "--timeseries") {
            options.time_series_path = value;
        } else if (argument == "--duration") {
            options.duration_seconds = std::stod(value);
        } else if (argument == "--tenant-a-iops") {
            options.tenant_a_iops = std::stod(value);
        } else if (argument == "--tenant-b-iops") {
            options.tenant_b_iops = std::stod(value);
        } else if (argument == "--tenant-a-spike-after") {
            options.tenant_a_spike_after_seconds = std::stod(value);
        } else if (argument == "--tenant-a-spike-multiplier") {
            options.tenant_a_spike_multiplier = std::stod(value);
        } else if (argument == "--tenant-a-request-kib") {
            options.tenant_a_request_kib = static_cast<std::size_t>(std::stoull(value));
        } else if (argument == "--tenant-b-request-kib") {
            options.tenant_b_request_kib = static_cast<std::size_t>(std::stoull(value));
        } else if (argument == "--tenant-a-access") {
            options.tenant_a_access = value;
        } else if (argument == "--tenant-b-access") {
            options.tenant_b_access = value;
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
    const auto positive_finite = [](double value) {
        return std::isfinite(value) && value > 0.0;
    };
    const bool scheduler_valid = options.scheduler == "fifo" ||
        options.scheduler == "token-bucket" || options.scheduler == "drr";
    const bool device_valid = options.device == "simulated" || options.device == "file";
    const bool durability_valid =
        options.file_durability == "buffered" || options.file_durability == "fsync";
    const bool access_valid =
        (options.tenant_a_access == "random" || options.tenant_a_access == "sequential") &&
        (options.tenant_b_access == "random" || options.tenant_b_access == "sequential");
    const bool spike_valid = std::isfinite(options.tenant_a_spike_after_seconds) &&
        positive_finite(options.tenant_a_spike_multiplier) &&
        (options.tenant_a_spike_after_seconds < 0.0 ||
        (options.tenant_a_spike_after_seconds > 0.0 &&
         options.tenant_a_spike_after_seconds < options.duration_seconds &&
         options.tenant_a_spike_multiplier > 0.0));
    const bool rates_valid = positive_finite(options.duration_seconds) &&
        positive_finite(options.tenant_a_iops) && positive_finite(options.tenant_b_iops) &&
        positive_finite(options.device_iops) && positive_finite(options.device_mib_per_sec);
    const bool limits_valid = positive_finite(options.tenant_a_limit_iops) &&
        positive_finite(options.tenant_b_limit_iops) &&
        positive_finite(options.tenant_a_limit_mib_per_sec) &&
        positive_finite(options.tenant_b_limit_mib_per_sec) &&
        std::isfinite(options.burst_requests) && options.burst_requests >= 1.0 &&
        options.tenant_a_weight > 0 && options.tenant_b_weight > 0 &&
        options.drr_quantum_bytes > 0;
    constexpr std::uint64_t kBytesPerMebibyte = 1024ULL * 1024ULL;
    const bool file_size_valid = options.file_size_mib > 0 &&
        options.file_size_mib <=
            std::numeric_limits<std::uint64_t>::max() / kBytesPerMebibyte;
    const bool request_sizes_valid = options.tenant_a_request_kib > 0 &&
        options.tenant_b_request_kib > 0 &&
        options.tenant_a_request_kib <=
            std::numeric_limits<std::size_t>::max() / 1024U &&
        options.tenant_b_request_kib <=
            std::numeric_limits<std::size_t>::max() / 1024U;
    if (!scheduler_valid || !device_valid || !durability_valid || !access_valid || !spike_valid ||
        !rates_valid || options.lanes == 0 || !file_size_valid ||
        options.file_path.empty() || !limits_valid || !request_sizes_valid) {
        throw std::invalid_argument("invalid scheduler or non-positive benchmark setting");
    }
    return options;
}

void write_time_series(
    const std::string& path,
    const std::string& scheduler,
    const std::string& device,
    const std::vector<ioshield::TimeSeriesPoint>& points) {
    if (path.empty()) {
        return;
    }
    std::ofstream output(path);
    if (!output) {
        throw std::runtime_error("unable to open time-series output: " + path);
    }
    output << "scheduler,device,window,tenant,completed,errors,ops_per_sec,mib_per_sec,"
              "total_p95_us,total_p99_us\n";
    output << std::fixed << std::setprecision(2);
    for (const auto& point : points) {
        output << scheduler << ',' << device << ',' << point.window_index << ','
               << point.tenant_id << ',' << point.completed << ',' << point.errors << ','
               << point.operations_per_second << ',' << point.mebibytes_per_second << ','
               << point.total_p95_us << ',' << point.total_p99_us << '\n';
    }
}

}  // namespace

int main(int argc, char** argv) {
    using namespace std::chrono_literals;
    using namespace ioshield;

    try {
        const Options options = parse_options(argc, argv);
        const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::duration<double>(options.duration_seconds));
        constexpr std::uint64_t kBytesPerMebibyteInteger = 1024ULL * 1024ULL;
        const std::uint64_t working_set_bytes = options.device == "file"
            ? options.file_size_mib * kBytesPerMebibyteInteger
            : 1ULL << 30;
        const std::size_t tenant_a_request_bytes = options.tenant_a_request_kib * 1024U;
        const std::size_t tenant_b_request_bytes = options.tenant_b_request_kib * 1024U;
        if (tenant_a_request_bytes > working_set_bytes ||
            tenant_b_request_bytes > working_set_bytes) {
            throw std::invalid_argument("request size exceeds device working set");
        }

        WorkloadConfig tenant_a{
            .tenant_id = "tenant-a-noisy",
            .offered_iops = options.tenant_a_iops,
            .duration = duration,
            .request_size_bytes = tenant_a_request_bytes,
            .working_set_bytes = working_set_bytes,
            .read_ratio = 0.8,
            .arrival_pattern = ArrivalPattern::Fixed,
            .access_pattern = options.tenant_a_access == "random"
                ? AccessPattern::Random
                : AccessPattern::Sequential,
            .seed = options.seed,
            .request_id_base = 1,
        };
        WorkloadConfig tenant_b{
            .tenant_id = "tenant-b-latency",
            .offered_iops = options.tenant_b_iops,
            .duration = duration,
            .request_size_bytes = tenant_b_request_bytes,
            .working_set_bytes = working_set_bytes,
            .read_ratio = 1.0,
            .arrival_pattern = ArrivalPattern::Fixed,
            .access_pattern = options.tenant_b_access == "random"
                ? AccessPattern::Random
                : AccessPattern::Sequential,
            .seed = options.seed + 1,
            .request_id_base = 1'000'000'000ULL,
        };

        std::vector<ScheduledRequest> tenant_a_requests;
        if (options.tenant_a_spike_after_seconds < 0.0) {
            tenant_a_requests = WorkloadGenerator::generate(tenant_a);
        } else {
            const auto spike_after = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::duration<double>(options.tenant_a_spike_after_seconds));
            tenant_a_requests = WorkloadGenerator::generate_phased(
                tenant_a,
                {
                    WorkloadPhase{0ns, spike_after, options.tenant_a_iops},
                    WorkloadPhase{
                        spike_after,
                        duration - spike_after,
                        options.tenant_a_iops * options.tenant_a_spike_multiplier},
                });
        }
        auto workload = WorkloadGenerator::merge({
            std::move(tenant_a_requests),
            WorkloadGenerator::generate(tenant_b),
        });

        constexpr double kBytesPerMebibyte = 1024.0 * 1024.0;
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
                        options.burst_requests * static_cast<double>(tenant_a_request_bytes),
                    },
                    {
                        "tenant-b-latency",
                        options.tenant_b_limit_iops,
                        options.burst_requests,
                        options.tenant_b_limit_mib_per_sec * kBytesPerMebibyte,
                        options.burst_requests * static_cast<double>(tenant_b_request_bytes),
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
                        options.burst_requests * static_cast<double>(tenant_a_request_bytes),
                    },
                    {
                        "tenant-b-latency",
                        options.tenant_b_weight,
                        options.tenant_b_limit_iops,
                        options.burst_requests,
                        options.tenant_b_limit_mib_per_sec * kBytesPerMebibyte,
                        options.burst_requests * static_cast<double>(tenant_b_request_bytes),
                    },
                },
                options.drr_quantum_bytes);
        }

        MetricsCollector metrics;
        std::unique_ptr<IBlockDevice> device;
        std::string device_label;
        if (options.device == "simulated") {
            device = std::make_unique<SimulatedBlockDevice>(SimulatedDeviceConfig{
                .max_iops = options.device_iops,
                .max_bandwidth_mib_per_sec = options.device_mib_per_sec,
                .base_latency = 100us,
                .lanes = options.lanes,
                .queue_depth = options.lanes,
            });
            device_label = "simulated";
        } else {
            const auto durability = options.file_durability == "fsync"
                ? FileDurability::FsyncPerWrite
                : FileDurability::Buffered;
            device = std::make_unique<FileBlockDevice>(FileDeviceConfig{
                .path = options.file_path,
                .size_bytes = working_set_bytes,
                .lanes = options.lanes,
                .queue_depth = options.lanes,
                .durability = durability,
            });
            device_label = options.file_durability == "fsync" ? "file-fsync" : "file-buffered";
        }

        std::thread dispatcher([&] {
            while (auto request = scheduler->wait_dequeue()) {
                if (!device->submit(
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
            const IORequest metric_request = scheduled.request;
            metrics.record_submission(metric_request);
            if (!scheduler->enqueue(std::move(scheduled.request))) {
                metrics.record_rejection(metric_request);
                throw std::runtime_error("scheduler closed while producing workload");
            }
        }

        const auto measurement_ended_at = started_at + duration;
        std::this_thread::sleep_until(measurement_ended_at);
        const auto measurement_elapsed = measurement_ended_at - started_at;
        const auto measurement_summaries = metrics.summarize(measurement_elapsed);
        const auto time_series = metrics.summarize_windows(
            started_at,
            measurement_ended_at,
            1s);
        const auto device_stats = device->stats();

        std::vector<double> allocations;
        std::vector<double> normalized_allocations;
        allocations.reserve(measurement_summaries.size());
        normalized_allocations.reserve(measurement_summaries.size());
        for (const auto& summary : measurement_summaries) {
            allocations.push_back(summary.mebibytes_per_second);
            double weight = 1.0;
            if (options.scheduler == "drr") {
                weight = summary.tenant_id == "tenant-a-noisy"
                    ? static_cast<double>(options.tenant_a_weight)
                    : static_cast<double>(options.tenant_b_weight);
            }
            normalized_allocations.push_back(summary.mebibytes_per_second / weight);
        }
        const double fairness = jains_fairness(allocations);
        const double weighted_fairness = jains_fairness(normalized_allocations);
        const double elapsed_ns = static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(measurement_elapsed).count());
        const double device_utilization = elapsed_ns > 0.0 && device_stats.lanes > 0
            ? std::min(
                  1.0,
                  static_cast<double>(device_stats.busy_time_ns) /
                      (elapsed_ns * static_cast<double>(device_stats.lanes)))
            : 0.0;
        write_time_series(options.time_series_path, options.scheduler, device_label, time_series);

        scheduler->close();
        dispatcher.join();
        device->wait_idle();
        device->close();

        std::cout << "scheduler,device,tenant,submitted,rejected,completed,errors,ops_per_sec,"
                     "mib_per_sec,queue_p50_us,queue_p95_us,queue_p99_us,service_p50_us,"
                     "service_p95_us,service_p99_us,total_p50_us,total_p95_us,total_p99_us,"
                     "device_utilization,jain_fairness,weighted_jain_fairness\n";
        std::cout << std::fixed << std::setprecision(2);
        for (const auto& summary : measurement_summaries) {
            std::cout << options.scheduler << ',' << device_label << ',' << summary.tenant_id << ','
                      << summary.submitted << ',' << summary.rejected << ',' << summary.completed << ','
                      << summary.errors << ',' << summary.operations_per_second << ','
                      << summary.mebibytes_per_second << ',' << summary.queue_p50_us << ','
                      << summary.queue_p95_us << ',' << summary.queue_p99_us << ','
                      << summary.service_p50_us << ',' << summary.service_p95_us << ','
                      << summary.service_p99_us << ','
                      << summary.total_p50_us << ',' << summary.total_p95_us << ','
                      << summary.total_p99_us << ',' << device_utilization << ',' << fairness << ','
                      << weighted_fairness << '\n';
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

