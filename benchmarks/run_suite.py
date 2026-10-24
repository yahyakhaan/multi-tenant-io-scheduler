#!/usr/bin/env python3
"""Run IOShield's reproducible noisy-neighbor benchmark matrix."""

from __future__ import annotations

import argparse
import csv
import io
import statistics
import subprocess
from pathlib import Path


POLICIES = ("fifo", "token-bucket", "drr")
TENANT_B = "tenant-b-latency"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("build/ioshield_benchmark"))
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark-results"))
    parser.add_argument("--duration", type=float, default=6.0)
    parser.add_argument("--spike-after", type=float, default=3.0)
    parser.add_argument("--repetitions", type=int, default=1)
    args = parser.parse_args()
    if args.duration <= 0 or not 0 < args.spike_after < args.duration:
        parser.error("--spike-after must be inside the positive benchmark duration")
    if args.repetitions <= 0:
        parser.error("--repetitions must be positive")
    return args


def benchmark_command(
    binary: Path,
    policy: str,
    duration: float,
    spike_after: float,
    time_series_path: Path,
    seed: int,
) -> list[str]:
    return [
        str(binary),
        "--scheduler", policy,
        "--device", "simulated",
        "--duration", str(duration),
        "--tenant-a-iops", "2800",
        "--tenant-b-iops", "1000",
        "--tenant-a-spike-after", str(spike_after),
        "--tenant-a-spike-multiplier", "2",
        "--tenant-a-request-kib", "64",
        "--tenant-b-request-kib", "4",
        "--tenant-a-access", "sequential",
        "--tenant-b-access", "random",
        "--device-iops", "5000",
        "--device-mibps", "250",
        "--tenant-a-limit-iops", "2800",
        "--tenant-b-limit-iops", "1000",
        "--tenant-a-limit-mibps", "175",
        "--tenant-b-limit-mibps", "8",
        "--burst-requests", "64",
        "--tenant-a-weight", "36",
        "--tenant-b-weight", "1",
        "--drr-quantum-bytes", "4096",
        "--lanes", "1",
        "--seed", str(seed),
        "--timeseries", str(time_series_path),
    ]


def mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


def analyze(
    summaries: list[dict[str, str]],
    series: list[dict[str, str]],
    spike_after: float,
) -> list[dict[str, float | str]]:
    results: list[dict[str, float | str]] = []
    for policy in POLICIES:
        policy_summaries = [row for row in summaries if row["scheduler"] == policy]
        runs = sorted({int(row["run"]) for row in policy_summaries})
        before_p95: list[float] = []
        after_p95: list[float] = []
        before_iops: list[float] = []
        after_iops: list[float] = []
        throughput: list[float] = []
        utilization: list[float] = []
        fairness: list[float] = []

        for run in runs:
            run_series = [
                row for row in series
                if row["scheduler"] == policy
                and int(row["run"]) == run
                and row["tenant"] == TENANT_B
            ]
            pre = [row for row in run_series if int(row["window"]) + 1 <= spike_after]
            post = [row for row in run_series if int(row["window"]) >= spike_after]
            before_p95.append(mean([float(row["total_p95_us"]) for row in pre]))
            after_p95.append(mean([float(row["total_p95_us"]) for row in post]))
            before_iops.append(mean([float(row["ops_per_sec"]) for row in pre]))
            after_iops.append(mean([float(row["ops_per_sec"]) for row in post]))

            run_summaries = [row for row in policy_summaries if int(row["run"]) == run]
            throughput.append(sum(float(row["ops_per_sec"]) for row in run_summaries))
            utilization.append(float(run_summaries[0]["device_utilization"]))
            fairness.append(float(run_summaries[0]["weighted_jain_fairness"]))

        pre_p95 = mean(before_p95)
        post_p95 = mean(after_p95)
        delta = ((post_p95 / pre_p95) - 1.0) * 100.0 if pre_p95 else 0.0
        results.append({
            "scheduler": policy,
            "tenant_b_pre_p95_us": pre_p95,
            "tenant_b_post_p95_us": post_p95,
            "tenant_b_p95_change_percent": delta,
            "tenant_b_pre_iops": mean(before_iops),
            "tenant_b_post_iops": mean(after_iops),
            "total_iops": mean(throughput),
            "device_utilization": mean(utilization),
            "weighted_jain_fairness": mean(fairness),
        })
    return results


def write_summary(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_report(
    path: Path,
    analyzed: list[dict[str, float | str]],
    duration: float,
    spike_after: float,
    repetitions: int,
) -> None:
    lines = [
        "# IOShield noisy-neighbor benchmark",
        "",
        f"Tenant A issues 64 KiB sequential I/O at 2,800 IOPS, then doubles its load "
        f"at {spike_after:g}s. Tenant B continuously issues 4 KiB random reads at "
        f"1,000 IOPS. The simulated device is bounded at 5,000 IOPS and 250 MiB/s. "
        f"Each scheduler ran for {duration:g}s with {repetitions} repetition(s).",
        "",
        "The p95 values below are means of the one-second p95 windows before and after "
        "the spike. Lower p95 growth means stronger noisy-neighbor isolation.",
        "",
        "| Scheduler | B p95 before (us) | B p95 after (us) | p95 change | "
        "B IOPS before | B IOPS after | Total IOPS | Utilization | Weighted Jain |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in analyzed:
        lines.append(
            f"| {row['scheduler']} | {row['tenant_b_pre_p95_us']:.2f} | "
            f"{row['tenant_b_post_p95_us']:.2f} | "
            f"{row['tenant_b_p95_change_percent']:+.1f}% | "
            f"{row['tenant_b_pre_iops']:.1f} | {row['tenant_b_post_iops']:.1f} | "
            f"{row['total_iops']:.1f} | {row['device_utilization']:.3f} | "
            f"{row['weighted_jain_fairness']:.3f} |"
        )
    lines.extend([
        "",
        "These are simulator results for policy comparison, not physical-disk claims. "
        "Raw aggregate and per-second CSV files are stored beside this report.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def write_svg(path: Path, analyzed: list[dict[str, float | str]]) -> None:
    width, height = 900, 460
    left, top, chart_height = 90, 55, 310
    values = [float(row[key]) for row in analyzed for key in (
        "tenant_b_pre_p95_us", "tenant_b_post_p95_us")]
    maximum = max(values) if values else 1.0
    colors = ("#5b8ff9", "#e8684a")
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<text x="450" y="28" text-anchor="middle" font-family="sans-serif" '
        'font-size="20" font-weight="bold">Tenant B p95 latency around Tenant A load spike</text>',
        f'<line x1="{left}" y1="{top + chart_height}" x2="850" '
        f'y2="{top + chart_height}" stroke="#333"/>',
    ]
    group_width = 220
    for index, row in enumerate(analyzed):
        center = left + 120 + index * group_width
        for bar_index, key in enumerate(("tenant_b_pre_p95_us", "tenant_b_post_p95_us")):
            value = float(row[key])
            bar_height = (value / maximum) * (chart_height - 20)
            x = center - 48 + bar_index * 62
            y = top + chart_height - bar_height
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="48" height="{bar_height:.1f}" '
                f'fill="{colors[bar_index]}" rx="3"/>'
            )
            parts.append(
                f'<text x="{x + 24:.1f}" y="{max(45.0, y - 6):.1f}" text-anchor="middle" '
                f'font-family="sans-serif" font-size="11">{value:.0f}</text>'
            )
        parts.append(
            f'<text x="{center:.1f}" y="{top + chart_height + 26}" text-anchor="middle" '
            f'font-family="sans-serif" font-size="14">{row["scheduler"]}</text>'
        )
    parts.extend([
        '<rect x="300" y="420" width="14" height="14" fill="#5b8ff9"/>',
        '<text x="321" y="432" font-family="sans-serif" font-size="13">Before spike</text>',
        '<rect x="450" y="420" width="14" height="14" fill="#e8684a"/>',
        '<text x="471" y="432" font-family="sans-serif" font-size="13">After spike</text>',
        '</svg>',
    ])
    path.write_text("\n".join(parts), encoding="utf-8")


def main() -> int:
    args = parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"benchmark binary not found: {binary}; build the project first")

    output_dir = args.output_dir.resolve()
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    summaries: list[dict[str, str]] = []
    series: list[dict[str, str]] = []

    for run in range(1, args.repetitions + 1):
        for policy in POLICIES:
            time_series_path = raw_dir / f"{policy}-run-{run}-timeseries.csv"
            command = benchmark_command(
                binary, policy, args.duration, args.spike_after, time_series_path, 41 + run)
            print(f"running {policy} ({run}/{args.repetitions})", flush=True)
            completed = subprocess.run(command, check=True, capture_output=True, text=True)
            run_summaries = list(csv.DictReader(io.StringIO(completed.stdout)))
            for row in run_summaries:
                row["run"] = str(run)
                summaries.append(row)
            for row in load_csv(time_series_path):
                row["run"] = str(run)
                series.append(row)

    write_summary(output_dir / "summary.csv", summaries)
    write_summary(output_dir / "timeseries.csv", series)
    analyzed = analyze(summaries, series, args.spike_after)
    write_report(
        output_dir / "report.md",
        analyzed,
        args.duration,
        args.spike_after,
        args.repetitions,
    )
    write_svg(output_dir / "tenant-b-p95.svg", analyzed)
    print(f"report: {output_dir / 'report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
