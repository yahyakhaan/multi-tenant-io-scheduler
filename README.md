# IOShield

IOShield is a C++20 multi-tenant I/O QoS scheduler. It is designed to measure
how queueing, rate limiting, and weighted fair scheduling affect latency
isolation and device utilization under contention.

## Implemented phases

Phase 1 provides the FIFO baseline:

- thread-safe FIFO scheduling;
- deterministic open-loop workload generation;
- fixed and Poisson arrival patterns;
- random and sequential access patterns;
- a configurable asynchronous simulated block device;
- per-tenant throughput and latency percentiles; and
- a benchmark CLI and dependency-free test executable.

Phase 2 adds per-tenant quota enforcement:

- independent operations-per-second and bytes-per-second token buckets;
- configurable burst capacity for both resources;
- explicit-time token accounting with deterministic unit tests;
- per-tenant queues with round-robin selection among eligible tenants;
- condition-variable deadline waits instead of polling; and
- head-of-line isolation when one tenant is rate limited.

Phase 3 adds weighted, byte-aware fair scheduling:

- Deficit Round Robin with configurable tenant weights;
- byte-denominated quanta for variable request sizes;
- work-conserving service when other tenants are idle;
- quota-aware tenant skipping without busy waiting;
- starvation protection for requests larger than one quantum; and
- concurrent producer and clean-shutdown validation.

Phase 4 adds real-I/O validation and benchmark observability:

- a bounded, multithreaded file-backed block device using `pread`/`pwrite`;
- buffered writes or `fsync` after every write for a durability stress mode;
- submission, rejection, completion, and I/O-error counters;
- p50/p95/p99 queue, service, and end-to-end latency;
- device utilization plus raw and weight-normalized Jain's fairness indices; and
- optional one-second per-tenant CSV time series for plotting load spikes.

Phase 5 turns those components into reproducible evidence:

- deterministic multi-phase workloads with mid-run rate changes;
- configurable request sizes and random or sequential access per tenant;
- an automated noisy-neighbor comparison across FIFO, token bucket, and DRR;
- generated raw CSV, a Markdown results report, and an SVG latency chart;
- regression tests for phased generation and an end-to-end spike scenario; and
- Linux and macOS CI builds with the complete test suite.

The simulated device is intentionally bounded. Its submission queue applies
backpressure so future scheduling policies retain control over which request is
dispatched next.

## Build

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Run the baseline benchmark:

```sh
./build/ioshield_benchmark
```

Run the same workload with per-tenant limits:

```sh
./build/ioshield_benchmark --scheduler token-bucket
```

Run weighted Deficit Round Robin:

```sh
./build/ioshield_benchmark --scheduler drr --tenant-a-weight 1 --tenant-b-weight 2
```

Run a single noisy-neighbor spike directly:

```sh
./build/ioshield_benchmark \
  --scheduler drr \
  --duration 6 \
  --tenant-a-iops 2800 \
  --tenant-a-spike-after 3 \
  --tenant-a-spike-multiplier 2 \
  --tenant-a-request-kib 64 \
  --tenant-a-access sequential \
  --tenant-b-request-kib 4 \
  --tenant-b-access random \
  --timeseries tenant-timeseries.csv
```

Run the complete FIFO/token-bucket/DRR experiment and generate a report:

```sh
python3 benchmarks/run_suite.py --binary build/ioshield_benchmark
```

Results are written to `benchmark-results/`: `report.md`, an SVG comparison,
combined aggregate and time-series CSVs, and the individual raw runs. See
[`benchmarks/README.md`](benchmarks/README.md) for the methodology.

Run the same scheduler against a real file. Use a disposable path: the file is
created if necessary and resized to the requested capacity.

```sh
./build/ioshield_benchmark \
  --scheduler drr \
  --device file \
  --file-path /tmp/ioshield.dat \
  --file-size-mib 1024 \
  --file-durability buffered \
  --timeseries /tmp/ioshield-timeseries.csv
```

`--file-durability fsync` forces an `fsync` after each write. This is useful as
a correctness and durability stress check, but it is intentionally much slower
than buffered mode.

Useful options:

```text
--scheduler fifo|token-bucket|drr
--device simulated|file
--file-path PATH
--file-size-mib SIZE
--file-durability buffered|fsync
--timeseries PATH
--duration SECONDS
--tenant-a-iops RATE
--tenant-b-iops RATE
--tenant-a-spike-after SECONDS
--tenant-a-spike-multiplier FACTOR
--tenant-a-request-kib SIZE
--tenant-b-request-kib SIZE
--tenant-a-access random|sequential
--tenant-b-access random|sequential
--device-iops RATE
--device-mibps RATE
--tenant-a-limit-iops RATE
--tenant-b-limit-iops RATE
--tenant-a-limit-mibps RATE
--tenant-b-limit-mibps RATE
--burst-requests COUNT
--tenant-a-weight WEIGHT
--tenant-b-weight WEIGHT
--drr-quantum-bytes BYTES
--lanes COUNT
--seed VALUE
```

The benchmark emits an aggregate CSV report to standard output. When
`--timeseries` is supplied, it also writes one-second throughput and tail-latency
windows to that path. The default workload deliberately offers more I/O than
the simulated device can process, exposing FIFO's lack of tenant isolation.
Reported throughput, latency, utilization, and fairness cover the configured
offered-load window; IOShield then drains all remaining work and verifies that
every accepted request completed.

## Current limitations

The simulator is the reproducible source of truth for policy comparisons; it
does not model a specific physical disk. The file backend verifies the complete
asynchronous I/O path, but its numbers depend on the operating system page
cache, filesystem, storage hardware, power state, and other host activity.
Neither backend should be presented as a production cloud-disk measurement.

