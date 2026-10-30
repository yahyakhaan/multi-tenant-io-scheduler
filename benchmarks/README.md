# Benchmark methodology

The benchmark suite answers one question: when a bandwidth-heavy tenant doubles
its offered load, can IOShield preserve the latency and throughput of an
independent small-read tenant?

## Scenario

- Tenant A submits 64 KiB sequential I/O at 2,800 IOPS, then doubles to 5,600
  IOPS halfway through the run.
- Tenant B submits 4 KiB random reads at a constant 1,000 IOPS.
- The simulated device is limited to 5,000 IOPS and 250 MiB/s with one lane.
- Token-bucket and DRR runs cap Tenant A at its pre-spike rate. DRR uses a 36:1
  byte-service weight, matching the target 2,800:1,000 operation ratio after
  accounting for the tenants' 64 KiB and 4 KiB request sizes.
- FIFO receives the identical workload but does not enforce quotas or fairness.

The default run lasts six seconds and changes load at three seconds. The harness
runs FIFO, token bucket, and DRR sequentially with the same deterministic seed.
Use `--repetitions N` to average multiple runs.

## Metrics

The generated report compares:

- Tenant B's mean one-second p95 latency before and after the spike;
- Tenant B's completed IOPS before and after the spike;
- total completed IOPS during the offered-load window;
- device busy-time utilization; and
- the weight-normalized Jain fairness index.

The application records only completions inside the configured measurement
window for reported performance. It then drains every accepted request and
fails the run if any completion is missing.

## Reproduce

From the repository root:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
python3 benchmarks/run_suite.py --binary build/ioshield_benchmark
```

The harness uses only the Python standard library. Its outputs go to the ignored
`benchmark-results/` directory so machine-specific runs do not pollute commits.
The deterministic simulator enables controlled scheduler comparisons, while the
file backed device exercises the complete asynchronous I/O path on real storage.
