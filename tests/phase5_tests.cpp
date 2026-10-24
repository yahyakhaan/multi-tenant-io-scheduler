#include "ioshield/workload.hpp"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

void test_phased_rate_change() {
    using namespace std::chrono_literals;
    const ioshield::WorkloadConfig base{
        .tenant_id = "noisy-neighbor",
        .offered_iops = 10.0,
        .duration = 2s,
        .request_size_bytes = 4096,
        .working_set_bytes = 1ULL << 20,
        .read_ratio = 1.0,
        .arrival_pattern = ioshield::ArrivalPattern::Fixed,
        .access_pattern = ioshield::AccessPattern::Sequential,
        .seed = 7,
        .request_id_base = 100,
    };

    const auto requests = ioshield::WorkloadGenerator::generate_phased(
        base,
        {
            ioshield::WorkloadPhase{0s, 1s, 10.0},
            ioshield::WorkloadPhase{1s, 1s, 20.0},
        });

    expect(requests.size() == 30, "doubling the rate doubles requests in the second phase");
    expect(requests.front().scheduled_after == 0s, "first phase starts at time zero");
    expect(requests[10].scheduled_after == 1s, "second phase starts at the configured boundary");

    std::set<std::uint64_t> ids;
    auto previous = requests.front().scheduled_after;
    for (const auto& scheduled : requests) {
        ids.insert(scheduled.request.id);
        expect(scheduled.scheduled_after >= previous, "phased requests remain time ordered");
        previous = scheduled.scheduled_after;
    }
    expect(ids.size() == requests.size(), "request IDs stay unique across phases");
}

void test_invalid_phases_are_rejected() {
    using namespace std::chrono_literals;
    const ioshield::WorkloadConfig base{
        .tenant_id = "tenant",
        .offered_iops = 10.0,
        .duration = 1s,
        .request_size_bytes = 4096,
        .working_set_bytes = 1ULL << 20,
    };

    bool empty_rejected = false;
    try {
        static_cast<void>(ioshield::WorkloadGenerator::generate_phased(base, {}));
    } catch (const std::invalid_argument&) {
        empty_rejected = true;
    }
    expect(empty_rejected, "an empty phase list is rejected");

    bool negative_start_rejected = false;
    try {
        static_cast<void>(ioshield::WorkloadGenerator::generate_phased(
            base,
            {ioshield::WorkloadPhase{-1ns, 1s, 10.0}}));
    } catch (const std::invalid_argument&) {
        negative_start_rejected = true;
    }
    expect(negative_start_rejected, "a negative phase start is rejected");
}

}  // namespace

int main() {
    test_phased_rate_change();
    test_invalid_phases_are_rejected();

    if (failures != 0) {
        std::cerr << failures << " Phase 5 test assertion(s) failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "All Phase 5 tests passed\n";
    return EXIT_SUCCESS;
}
