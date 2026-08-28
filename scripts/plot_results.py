#!/usr/bin/env python3
"""Generate the plots referenced in plan §5 from a benchmark run's CSV.

Produces, per run directory:
  plots/throughput_comparison.svg
  plots/latency_cdf.svg
  plots/memory_footprint.svg

Falls back to a text summary when matplotlib is unavailable, so a benchmark run
is never blocked on an optional plotting dependency.
"""

from __future__ import annotations

import csv
import os
import sys
from collections import defaultdict

# Colour-blind-safe qualitative palette, consistent across all three plots so a
# candidate keeps the same colour whichever chart you are reading.
PALETTE = ["#4C78A8", "#F58518", "#54A24B", "#E45756", "#B279A2"]

# Percentile columns and the percentile each represents, for the CDF.
PERCENTILE_COLUMNS = [
    ("p50_us", 50.0),
    ("p90_us", 90.0),
    ("p95_us", 95.0),
    ("p99_us", 99.0),
    ("p999_us", 99.9),
    ("p9999_us", 99.99),
]


def load(csv_path: str) -> list[dict[str, str]]:
    with open(csv_path, newline="") as handle:
        return list(csv.DictReader(handle))


def group_by_candidate(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["candidate"]].append(row)
    for series in grouped.values():
        series.sort(key=lambda r: int(r["concurrency"]))
    return dict(grouped)


def text_summary(rows: list[dict[str, str]]) -> None:
    print("\n--- Benchmark summary ---")
    header = f"{'candidate':<24}{'conn':>8}{'rps':>12}{'p99':>12}{'rss MB':>10}"
    print(header)
    print("-" * len(header))
    for row in rows:
        print(
            f"{row['candidate']:<24}"
            f"{int(row['concurrency']):>8}"
            f"{float(row['rps']):>12.0f}"
            f"{int(row['p99_us']):>10} us"
            f"{float(row['rss_mb']):>10.1f}"
        )


def plot_all(rows: list[dict[str, str]], out_dir: str) -> bool:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed; skipping plots (pip install matplotlib)")
        return False

    os.makedirs(out_dir, exist_ok=True)
    grouped = group_by_candidate(rows)
    colours = {name: PALETTE[i % len(PALETTE)] for i, name in enumerate(sorted(grouped))}

    # --- 1. Throughput vs concurrency ------------------------------------
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for name, series in sorted(grouped.items()):
        ax.plot(
            [int(r["concurrency"]) for r in series],
            [float(r["rps"]) for r in series],
            marker="o",
            linewidth=2,
            label=name,
            color=colours[name],
        )
    ax.set_xscale("log")
    ax.set_xlabel("Concurrent connections")
    ax.set_ylabel("Requests / second")
    ax.set_title("Throughput vs. concurrency")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "throughput_comparison.svg"))
    plt.close(fig)

    # --- 2. Latency CDF ---------------------------------------------------
    # Drawn from the recorded percentiles rather than raw samples, so the curve
    # is exact at the points HdrHistogram reported and interpolated between.
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for name, series in sorted(grouped.items()):
        # Use the highest concurrency measured, where tails actually separate.
        row = series[-1]
        xs = [float(row[col]) / 1000.0 for col, _ in PERCENTILE_COLUMNS]
        ys = [pct for _, pct in PERCENTILE_COLUMNS]
        ax.plot(xs, ys, marker="o", linewidth=2, label=f"{name} (c={row['concurrency']})",
                color=colours[name])
    ax.set_xscale("log")
    ax.set_xlabel("Latency (ms, log scale)")
    ax.set_ylabel("Percentile")
    ax.set_ylim(45, 100.5)
    ax.set_title("Latency distribution")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(frameon=False, fontsize=9)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "latency_cdf.svg"))
    plt.close(fig)

    # --- 3. Memory footprint ---------------------------------------------
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for name, series in sorted(grouped.items()):
        ax.plot(
            [int(r["concurrency"]) for r in series],
            [float(r["rss_mb"]) for r in series],
            marker="s",
            linewidth=2,
            label=name,
            color=colours[name],
        )
    ax.set_xscale("log")
    ax.set_xlabel("Concurrent connections")
    ax.set_ylabel("Peak RSS (MiB)")
    ax.set_title("Memory footprint vs. connection count")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "memory_footprint.svg"))
    plt.close(fig)

    print(f"Wrote 3 plots to {out_dir}")
    return True


def main() -> int:
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "results/results.csv"
    if not os.path.exists(csv_path):
        print(f"Results CSV not found: {csv_path}")
        print("Run a benchmark first, e.g.:")
        print("  cargo run --release --bin lb-bench -- --all --quick")
        return 1

    rows = load(csv_path)
    if not rows:
        print(f"{csv_path} contains no measurements.")
        return 1

    text_summary(rows)
    # Plots land next to the CSV, in the run directory's plots/ subdirectory.
    out_dir = os.path.join(os.path.dirname(os.path.abspath(csv_path)), "plots")
    plot_all(rows, out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
