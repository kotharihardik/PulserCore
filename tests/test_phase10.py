"""Phase 10 Validation Tests: Multi-Process Chaos Experiments & CSV Metrics Harness."""

import csv
import os
import shutil
import tempfile
import pytest

from scripts.fault_harness import run_scenarios


@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp(prefix="pulser_test_phase10_")
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


class TestPhase10FaultInjectionHarness:
    """Official Phase 10 Done When Criterion:
    Each scenario runs with one command and produces a CSV of metrics.
    """

    def test_all_three_chaos_scenarios_and_csv_export(self, temp_dir):
        csv_path = os.path.join(temp_dir, "metrics.csv")

        results = run_scenarios(scenario="all", runs=1, output_path=csv_path)

        # Should produce 4 rows: consumer_crash_exclusive, consumer_crash_shared, broker_kill_sigkill, broker_zombie_sigstop
        assert len(results) == 4
        assert os.path.exists(csv_path)

        with open(csv_path, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        assert len(rows) == 4
        scenarios_seen = {row["scenario"]: row for row in rows}

        # 1. Consumer crash (Exclusive)
        sc1_excl = scenarios_seen["consumer_crash_exclusive"]
        assert int(sc1_excl["unique_delivered"]) == int(sc1_excl["total_published"])
        assert int(sc1_excl["redelivered_count"]) >= 1
        assert float(sc1_excl["recovery_latency_ms"]) >= 0
        assert sc1_excl["ordering_preserved"] == "True"

        # 2. Consumer crash (Shared)
        sc1_shared = scenarios_seen["consumer_crash_shared"]
        assert int(sc1_shared["unique_delivered"]) == int(sc1_shared["total_published"])
        assert int(sc1_shared["redelivered_count"]) >= 1
        assert float(sc1_shared["recovery_latency_ms"]) >= 0

        # 3. Broker kill (SIGKILL)
        sc2 = scenarios_seen["broker_kill_sigkill"]
        assert int(sc2["unique_delivered"]) == int(sc2["total_published"])
        assert float(sc2["recovery_latency_ms"]) > 0
        assert float(sc2["throughput_msgs_per_sec"]) > 0

        # 4. Zombie broker (SIGSTOP / SIGCONT)
        sc3 = scenarios_seen["broker_zombie_sigstop"]
        assert int(sc3["unique_delivered"]) == int(sc3["total_published"])
        assert sc3["zombie_fenced"] == "True"
        assert float(sc3["recovery_latency_ms"]) > 0

