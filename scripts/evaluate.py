"""Multi-run benchmark aggregator and chart generator for PulserCore (Phase 11)."""

import argparse
import csv
import os
import statistics
from collections import defaultdict
from typing import Dict, List

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from scripts.fault_harness import ExperimentResult, run_scenarios


def aggregate_csv(csv_path: str) -> List[Dict[str, str]]:
    """Computes mean, min, max, and stdev metrics grouped by scenario."""
    by_scenario: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    with open(csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            by_scenario[row["scenario"]].append(row)

    summary_rows: List[Dict[str, str]] = []
    for scenario, rows in by_scenario.items():
        recs = [float(r["recovery_latency_ms"]) for r in rows if float(r["recovery_latency_ms"]) >= 0]
        redelivs = [int(r["redelivered_count"]) for r in rows]
        dups = [int(r["duplicates_seen"]) for r in rows]
        tputs = [float(r["throughput_msgs_per_sec"]) for r in rows]
        loss_count = sum(int(r["total_published"]) - int(r["unique_delivered"]) for r in rows)

        summary_rows.append({
            "scenario": scenario,
            "runs": str(len(rows)),
            "mode": rows[0]["mode"],
            "mean_recovery_ms": f"{statistics.mean(recs):.2f}" if recs else "0.00",
            "min_recovery_ms": f"{min(recs):.2f}" if recs else "0.00",
            "max_recovery_ms": f"{max(recs):.2f}" if recs else "0.00",
            "stdev_recovery_ms": f"{statistics.stdev(recs):.2f}" if len(recs) > 1 else "0.00",
            "mean_redeliveries": f"{statistics.mean(redelivs):.2f}",
            "mean_duplicates": f"{statistics.mean(dups):.2f}",
            "mean_throughput_msg_s": f"{statistics.mean(tputs):.2f}",
            "total_lost_messages": str(loss_count),
            "ordering_preserved_all_runs": str(all(r["ordering_preserved"] == "True" for r in rows)),
            "zombie_fenced_all_runs": str(all(r["zombie_fenced"] == "True" for r in rows)),
        })
    return summary_rows


def write_summary_csv(summary_rows: List[Dict[str, str]], summary_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(summary_path)), exist_ok=True)
    if not summary_rows:
        return
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)


def generate_plots(summary_rows: List[Dict[str, str]], out_dir: str) -> List[str]:
    """Generates recovery latency and redelivery/throughput PNG charts."""
    os.makedirs(out_dir, exist_ok=True)
    labels_map = {
        "consumer_crash_exclusive": "Consumer Crash\n(Exclusive)",
        "consumer_crash_shared": "Consumer Crash\n(Shared)",
        "broker_kill_sigkill": "Broker Crash\n(SIGKILL)",
        "broker_zombie_sigstop": "Zombie Broker\n(SIGSTOP/CONT)",
    }

    scenarios = [labels_map.get(r["scenario"], r["scenario"]) for r in summary_rows]
    mean_rec = [float(r["mean_recovery_ms"]) for r in summary_rows]
    std_rec = [float(r["stdev_recovery_ms"]) for r in summary_rows]
    mean_redeliv = [float(r["mean_redeliveries"]) for r in summary_rows]
    mean_tput = [float(r["mean_throughput_msg_s"]) for r in summary_rows]

    # Plot 1: Recovery Latency (ms)
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    colors = ["#2563eb", "#0891b2", "#dc2626", "#7c3aed"]
    bars = ax.bar(scenarios, mean_rec, yerr=std_rec, capsize=5, color=colors[:len(scenarios)], alpha=0.9)
    max_y = max((v + s for v, s in zip(mean_rec, std_rec)), default=100.0)
    ax.set_ylim(0, max_y * 1.16)
    ax.set_ylabel("Recovery Latency (ms)", fontsize=11, fontweight="bold")
    ax.set_title("Failover & Crash Recovery Latency (Mean ± StdDev)", fontsize=12, fontweight="bold")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    for bar, val, std in zip(bars, mean_rec, std_rec):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            val + std + max_y * 0.025,
            f"{val:.1f} ms",
            ha="center",
            va="bottom",
            fontsize=9.5,
            fontweight="bold"
        )
    plt.tight_layout()
    latency_path = os.path.join(out_dir, "recovery_latency.png")
    plt.savefig(latency_path, dpi=150)
    plt.close(fig)

    # Plot 2: Redeliveries & Throughput
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))

    bars1 = ax1.bar(scenarios, mean_redeliv, color="#ea580c", alpha=0.88)
    max_redeliv = max(mean_redeliv, default=1.0)
    ax1.set_ylim(0, max_redeliv * 1.15)
    ax1.set_ylabel("Mean Redelivered Messages", fontsize=10, fontweight="bold")
    ax1.set_title("Redeliveries Triggered per Failure", fontsize=11, fontweight="bold")
    ax1.grid(axis="y", linestyle="--", alpha=0.4)
    ax1.tick_params(axis="x", labelsize=8.5)
    for bar, val in zip(bars1, mean_redeliv):
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_redeliv * 0.025,
            f"{val:.1f}",
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold"
        )

    bars2 = ax2.bar(scenarios, mean_tput, color="#059669", alpha=0.88)
    max_tput = max(mean_tput, default=10.0)
    ax2.set_ylim(0, max_tput * 1.15)
    ax2.set_ylabel("Throughput (msgs/sec)", fontsize=10, fontweight="bold")
    ax2.set_title("End-to-End Throughput Under Failure", fontsize=11, fontweight="bold")
    ax2.grid(axis="y", linestyle="--", alpha=0.4)
    ax2.tick_params(axis="x", labelsize=8.5)
    for bar, val in zip(bars2, mean_tput):
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_tput * 0.025,
            f"{val:.1f}/s",
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold"
        )

    plt.tight_layout()
    redeliv_path = os.path.join(out_dir, "redeliveries_and_throughput.png")
    plt.savefig(redeliv_path, dpi=150)
    plt.close(fig)

    return [latency_path, redeliv_path]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run multi-trial evaluation and plot metrics.")
    parser.add_argument("--runs", type=int, default=3, help="Number of trials per scenario")
    parser.add_argument("--raw-csv", default="results/benchmark_runs.csv")
    parser.add_argument("--summary-csv", default="results/summary.csv")
    parser.add_argument("--plots-dir", default="results")
    parser.add_argument("--from-csv", default=None, help="Skip running experiments and plot from existing CSV")
    args = parser.parse_args()

    if args.from_csv:
        csv_source = args.from_csv
    else:
        print(f"Running all 4 chaos scenarios x {args.runs} trials each...")
        run_scenarios(scenario="all", runs=args.runs, output_path=args.raw_csv)
        csv_source = args.raw_csv

    summary_rows = aggregate_csv(csv_source)
    write_summary_csv(summary_rows, args.summary_csv)
    charts = generate_plots(summary_rows, args.plots_dir)

    print("\n=== Evaluation Summary ===")
    for r in summary_rows:
        print(
            f"  {r['scenario']:<26} | runs={r['runs']} | "
            f"rec_ms={r['mean_recovery_ms']} (min={r['min_recovery_ms']}, max={r['max_recovery_ms']}) | "
            f"redeliv={r['mean_redeliveries']} | tput={r['mean_throughput_msg_s']}/s | "
            f"lost={r['total_lost_messages']}"
        )
    print(f"\nGenerated summary CSV: {args.summary_csv}")
    for c in charts:
        print(f"Generated chart: {c}")


if __name__ == "__main__":
    main()

