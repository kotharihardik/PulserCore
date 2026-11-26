"""Phase 11 Validation Tests: Benchmark Aggregation, Chart Generation & End-to-End Artifacts."""

import csv
import os
import shutil
import tempfile
import pytest

from scripts.evaluate import aggregate_csv, generate_plots, write_summary_csv


@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp(prefix="pulser_test_phase11_")
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


class TestPhase11EvaluationArtifacts:
    """Verify multi-run aggregation, plotting, and presence of final report & benchmark artifacts."""

    def test_aggregate_and_plot_generation(self, temp_dir):
        sample_csv = os.path.join(temp_dir, "runs.csv")
        rows = [
            {
                "run_id": "1", "scenario": "consumer_crash_exclusive", "mode": "EXCLUSIVE",
                "total_published": "60", "unique_delivered": "60", "redelivered_count": "5",
                "duplicates_seen": "5", "recovery_latency_ms": "12.0", "publish_recovery_ms": "0.0",
                "deliver_recovery_ms": "12.0", "throughput_msgs_per_sec": "54.0",
                "ordering_preserved": "True", "zombie_fenced": "False", "notes": "run1"
            },
            {
                "run_id": "2", "scenario": "consumer_crash_exclusive", "mode": "EXCLUSIVE",
                "total_published": "60", "unique_delivered": "60", "redelivered_count": "5",
                "duplicates_seen": "5", "recovery_latency_ms": "14.0", "publish_recovery_ms": "0.0",
                "deliver_recovery_ms": "14.0", "throughput_msgs_per_sec": "56.0",
                "ordering_preserved": "True", "zombie_fenced": "False", "notes": "run2"
            },
            {
                "run_id": "1", "scenario": "broker_kill_sigkill", "mode": "EXCLUSIVE",
                "total_published": "80", "unique_delivered": "80", "redelivered_count": "0",
                "duplicates_seen": "0", "recovery_latency_ms": "705.0", "publish_recovery_ms": "695.0",
                "deliver_recovery_ms": "705.0", "throughput_msgs_per_sec": "30.0",
                "ordering_preserved": "True", "zombie_fenced": "False", "notes": "run1"
            },
        ]
        with open(sample_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

        summary = aggregate_csv(sample_csv)
        assert len(summary) == 2

        by_sc = {s["scenario"]: s for s in summary}
        assert by_sc["consumer_crash_exclusive"]["mean_recovery_ms"] == "13.00"
        assert by_sc["consumer_crash_exclusive"]["total_lost_messages"] == "0"

        summary_csv = os.path.join(temp_dir, "summary.csv")
        write_summary_csv(summary, summary_csv)
        assert os.path.exists(summary_csv)

        charts = generate_plots(summary, temp_dir)
        assert len(charts) == 2
        for c in charts:
            assert os.path.exists(c)
            assert os.path.getsize(c) > 0

    def test_final_project_deliverables_exist(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        expected = [
            "README.md",
            "REPORT.md",
            "protocol.md",
            "results/benchmark_runs.csv",
            "results/summary.csv",
            "results/recovery_latency.png",
            "results/redeliveries_and_throughput.png",
        ]
        for rel_path in expected:
            full_path = os.path.join(root, rel_path)
            assert os.path.exists(full_path), f"Missing expected deliverable: {rel_path}"
            assert os.path.getsize(full_path) > 0

