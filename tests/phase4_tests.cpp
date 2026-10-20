#include "ioshield/file_block_device.hpp"
#include "ioshield/metrics.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <system_error>
#include <utility>
#include <unistd.h>
#include <vector>

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

struct TemporaryFile {
    explicit TemporaryFile(std::string label) {
        path = std::filesystem::temp_directory_path() /
            ("ioshield-" + std::move(label) + "-" + std::to_string(::getpid()) + ".dat");
        std::error_code error;
        std::filesystem::remove(path, error);
    }

    ~TemporaryFile() {
        std::error_code error;
        std::filesystem::remove(path, error);
    }

    std::filesystem::path path;
};

ioshield::IORequest request(
    std::uint64_t id,
    ioshield::IOOperation operation,
    std::uint64_t offset = 0,
    std::size_t bytes = 4096,
    std::string tenant = "tenant") {
    return ioshield::IORequest{
        .id = id,
        .tenant_id = std::move(tenant),
        .operation = operation,
        .offset_bytes = offset,
        .size_bytes = bytes,
        .submitted_at = ioshield::Clock::now(),
    };
}

void test_file_device_reads_writes_and_stats() {
    TemporaryFile file("buffered");
    ioshield::FileBlockDevice device(ioshield::FileDeviceConfig{
        .path = file.path.string(),
        .size_bytes = 64 * 1024,
        .lanes = 2,
        .queue_depth = 4,
        .durability = ioshield::FileDurability::Buffered,
    });

    std::atomic<std::size_t> callbacks{0};
    std::atomic<std::size_t> successes{0};
    auto completion = [&](ioshield::IOCompletion result) {
        ++callbacks;
        if (result.status == ioshield::IOStatus::Ok) {
            ++successes;
        }
    };

    expect(device.submit(request(42, ioshield::IOOperation::Write, 4096), completion),
           "file device accepts write");
    device.wait_idle();
    expect(device.submit(request(43, ioshield::IOOperation::Read, 4096), completion),
           "file device accepts read");
    device.wait_idle();

    const auto stats = device.stats();
    expect(callbacks == 2, "each file request completes exactly once");
    expect(successes == 2, "valid file requests succeed");
    expect(stats.submitted == 2 && stats.completed == 2, "file device reports request counts");
    expect(stats.errors == 0, "file device reports no errors for valid I/O");
    expect(stats.busy_time_ns > 0, "file device records worker busy time");

    std::ifstream contents(file.path, std::ios::binary);
    contents.seekg(4096);
    std::vector<char> block(4096);
    contents.read(block.data(), static_cast<std::streamsize>(block.size()));
    expect(contents.gcount() == static_cast<std::streamsize>(block.size()),
           "written block can be read from backing file");
    expect(std::any_of(block.begin(), block.end(), [](char value) { return value != 0; }),
           "write operation stores deterministic non-zero data");

    device.close();
    expect(!device.submit(request(44, ioshield::IOOperation::Read), completion),
           "closed file device rejects submissions");
}

void test_file_device_rejects_out_of_range_request() {
    TemporaryFile file("range");
    ioshield::FileBlockDevice device(ioshield::FileDeviceConfig{
        .path = file.path.string(),
        .size_bytes = 8192,
    });
    ioshield::IOStatus status = ioshield::IOStatus::Ok;
    expect(device.submit(
               request(1, ioshield::IOOperation::Read, 8192, 4096),
               [&](ioshield::IOCompletion result) { status = result.status; }),
           "invalid file request produces a completion");
    expect(status == ioshield::IOStatus::InvalidRequest, "out-of-range request is rejected");
    const auto stats = device.stats();
    expect(stats.errors == 1 && stats.completed == 1, "invalid request is counted as an error");
    device.close();
}

void test_fsync_mode_completes_write() {
    TemporaryFile file("fsync");
    ioshield::FileBlockDevice device(ioshield::FileDeviceConfig{
        .path = file.path.string(),
        .size_bytes = 8192,
        .durability = ioshield::FileDurability::FsyncPerWrite,
    });
    ioshield::IOStatus status = ioshield::IOStatus::IOError;
    expect(device.submit(
               request(1, ioshield::IOOperation::Write),
               [&](ioshield::IOCompletion result) { status = result.status; }),
           "fsync write accepted");
    device.wait_idle();
    expect(status == ioshield::IOStatus::Ok, "fsync write completes successfully");
    device.close();
}

void test_metrics_time_series_and_fairness() {
    using namespace std::chrono_literals;
    ioshield::MetricsCollector metrics;
    const auto start = ioshield::Clock::now();

    auto first = request(1, ioshield::IOOperation::Read, 0, 4096, "a");
    first.submitted_at = start;
    metrics.record_submission(first);
    metrics.record(ioshield::IOCompletion{
        .request = first,
        .status = ioshield::IOStatus::Ok,
        .dispatched_at = start + 100us,
        .completed_at = start + 500ms,
    });

    auto second = request(2, ioshield::IOOperation::Read, 0, 8192, "b");
    second.submitted_at = start + 1s;
    metrics.record_submission(second);
    metrics.record_rejection(second);
    metrics.record(ioshield::IOCompletion{
        .request = second,
        .status = ioshield::IOStatus::IOError,
        .dispatched_at = start + 1s + 200us,
        .completed_at = start + 1500ms,
    });

    const auto summaries = metrics.summarize(2s);
    expect(summaries.size() == 2, "summary contains both tenants");
    expect(summaries[1].submitted == 1 && summaries[1].rejected == 1,
           "summary reports submission and rejection counts");
    expect(summaries[1].errors == 1, "summary reports I/O errors");
    expect(summaries[0].service_p50_us > 0.0, "summary separates service latency");

    const auto windows = metrics.summarize_windows(start, start + 2s, 1s);
    expect(windows.size() == 4, "two tenants over two windows produce four points");
    expect(windows[0].tenant_id == "a" && windows[0].completed == 1,
           "first tenant completion lands in first window");
    expect(windows[3].tenant_id == "b" && windows[3].completed == 1,
           "second tenant completion lands in second window");

    expect(std::abs(ioshield::jains_fairness({1.0, 1.0}) - 1.0) < 1e-12,
           "equal allocations have perfect Jain fairness");
    expect(std::abs(ioshield::jains_fairness({1.0, 0.0}) - 0.5) < 1e-12,
           "single-tenant allocation has expected Jain fairness");
}

}  // namespace

int main() {
    test_file_device_reads_writes_and_stats();
    test_file_device_rejects_out_of_range_request();
    test_fsync_mode_completes_write();
    test_metrics_time_series_and_fairness();

    if (failures != 0) {
        std::cerr << failures << " Phase 4 test assertion(s) failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "All Phase 4 tests passed\n";
    return EXIT_SUCCESS;
}
