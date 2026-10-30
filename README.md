# IOShield

IOShield is a C++20 I/O scheduler that prevents one tenant from monopolizing a
shared storage device.

It combines per tenant token bucket quotas with byte aware Deficit Round Robin
(DRR) scheduling, then measures the effect on throughput, fairness, and tail
latency under contention.

```text
Tenant workloads -> Per-tenant queues -> Token buckets -> DRR -> Block device
```

## Results

The included benchmark doubles a bandwidth heavy tenant's load halfway through
the run while a latency sensitive tenant continues issuing small reads.

In a representative local run:

- FIFO increased the protected tenant's p95 latency by **318%** and reduced its
  throughput from about **849 to 547 IOPS**.
- Token bucket and DRR scheduling held the tenant near **1,000 IOPS** with stable
  p95 latency.
- Weighted DRR achieved a **1.000 weight normalized Jain fairness index**.

Run the experiment yourself with `benchmarks/run_suite.py`; it generates the raw
CSV data, a Markdown report, and an SVG comparison chart.

## Highlights

- Concurrent FIFO, rate limited, and weighted DRR schedulers
- Independent per tenant IOPS and bandwidth token buckets
- Variable request sizes with random or sequential access patterns
- Asynchronous simulated and POSIX `pread`/`pwrite` block devices
- p50/p95/p99 queue, service, and end to end latency metrics
- Reproducible load spike benchmarks and Linux/macOS CI

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

## Run

Run the DRR scheduler:

```sh
./build/ioshield_benchmark --scheduler drr
```

Compare FIFO, token bucket, and DRR under the noisy neighbor workload:

```sh
python3 benchmarks/run_suite.py --binary build/ioshield_benchmark
```

The generated report is written to `benchmark-results/report.md`. For the full
scenario and metric definitions, see [benchmarks/README.md](benchmarks/README.md).

Run `./build/ioshield_benchmark --help` for workload, quota, device, and output
options.
