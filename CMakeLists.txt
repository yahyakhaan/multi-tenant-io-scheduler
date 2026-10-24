cmake_minimum_required(VERSION 3.20)

project(ioshield VERSION 0.1.0 LANGUAGES CXX)

option(IOSHIELD_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_library(ioshield_core
    src/drr_scheduler.cpp
    src/file_block_device.cpp
    src/fifo_scheduler.cpp
    src/metrics.cpp
    src/rate_limited_scheduler.cpp
    src/simulated_block_device.cpp
    src/token_bucket.cpp
    src/workload.cpp
)

target_include_directories(ioshield_core PUBLIC include)
target_link_libraries(ioshield_core PUBLIC Threads::Threads)

function(ioshield_enable_warnings target)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${target} PRIVATE
            -Wall -Wextra -Wpedantic -Wconversion -Wshadow
        )
    endif()
endfunction()

ioshield_enable_warnings(ioshield_core)

if(IOSHIELD_ENABLE_SANITIZERS AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(ioshield_core PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(ioshield_core PUBLIC -fsanitize=address,undefined)
endif()

add_executable(ioshield_benchmark apps/ioshield_benchmark.cpp)
target_link_libraries(ioshield_benchmark PRIVATE ioshield_core)
ioshield_enable_warnings(ioshield_benchmark)

enable_testing()
add_executable(ioshield_tests tests/phase1_tests.cpp)
target_link_libraries(ioshield_tests PRIVATE ioshield_core)
ioshield_enable_warnings(ioshield_tests)
add_test(NAME phase1 COMMAND ioshield_tests)

add_executable(ioshield_phase2_tests tests/phase2_tests.cpp)
target_link_libraries(ioshield_phase2_tests PRIVATE ioshield_core)
ioshield_enable_warnings(ioshield_phase2_tests)
add_test(NAME phase2 COMMAND ioshield_phase2_tests)

add_executable(ioshield_phase3_tests tests/phase3_tests.cpp)
target_link_libraries(ioshield_phase3_tests PRIVATE ioshield_core)
ioshield_enable_warnings(ioshield_phase3_tests)
add_test(NAME phase3 COMMAND ioshield_phase3_tests)

add_executable(ioshield_phase4_tests tests/phase4_tests.cpp)
target_link_libraries(ioshield_phase4_tests PRIVATE ioshield_core)
ioshield_enable_warnings(ioshield_phase4_tests)
add_test(NAME phase4 COMMAND ioshield_phase4_tests)

add_executable(ioshield_phase5_tests tests/phase5_tests.cpp)
target_link_libraries(ioshield_phase5_tests PRIVATE ioshield_core)
ioshield_enable_warnings(ioshield_phase5_tests)
add_test(NAME phase5 COMMAND ioshield_phase5_tests)

add_test(
    NAME phase5_spike_smoke
    COMMAND ioshield_benchmark
        --scheduler drr
        --duration 0.2
        --tenant-a-iops 100
        --tenant-b-iops 50
        --tenant-a-spike-after 0.1
        --tenant-a-spike-multiplier 2
        --tenant-a-request-kib 64
        --tenant-a-access sequential
        --tenant-a-limit-iops 1000
        --tenant-b-limit-iops 1000
        --tenant-a-limit-mibps 1000
        --tenant-b-limit-mibps 1000
)


