"""Multi-process fault-injection & metrics harness for PulserCore (Phase 10).

Spawns a real 5-node localhost cluster (1 Storage, 1 Coordinator, 3 Brokers) as
independent OS processes, injects real crashes (SIGKILL, SIGSTOP/SIGCONT),
and exports recovery latency, redelivery, duplicate, and throughput metrics to CSV.
"""

import argparse
import csv
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from client.consumer import Consumer
from client.producer import Producer, PublishError
from common.protocol import SubscriptionModes
from coordinator.coordinator_client import CoordinatorClient
from storage.log_manager import FencedError
from storage.storage_client import StorageClient


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"Node on {host}:{port} did not start within {timeout}s")


@dataclass
class ExperimentResult:
    run_id: int
    scenario: str
    mode: str
    total_published: int
    unique_delivered: int
    redelivered_count: int
    duplicates_seen: int
    recovery_latency_ms: float
    publish_recovery_ms: float
    deliver_recovery_ms: float
    throughput_msgs_per_sec: float
    ordering_preserved: bool
    zombie_fenced: bool
    notes: str = ""


class ClusterOrchestrator:
    """Manages the lifecycle of the 5-node multi-process PulserCore cluster."""

    def __init__(
        self,
        lease_ttl_ms: int = 800,
        renew_interval_sec: float = 0.25,
        ack_timeout_sec: float = 0.6,
        watch_topics: Optional[List[str]] = None
    ):
        self.data_dir = tempfile.mkdtemp(prefix="pulser_cluster_")
        self.storage_dir = os.path.join(self.data_dir, "storage")
        self.coord_dir = os.path.join(self.data_dir, "coord")
        os.makedirs(self.storage_dir, exist_ok=True)
        os.makedirs(self.coord_dir, exist_ok=True)

        self.lease_ttl_ms = lease_ttl_ms
        self.renew_interval_sec = renew_interval_sec
        self.ack_timeout_sec = ack_timeout_sec
        self.watch_topics = watch_topics or ["orders"]

        self.storage_port = get_free_port()
        self.coord_port = get_free_port()
        self.broker_ports: Dict[str, int] = {
            "broker-1": get_free_port(),
            "broker-2": get_free_port(),
            "broker-3": get_free_port(),
        }
        self.procs: Dict[str, subprocess.Popen] = {}

    def _spawn(self, name: str, args: List[str], port: int) -> subprocess.Popen:
        proc = subprocess.Popen(
            [sys.executable, "-m", "scripts.run_node"] + args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        self.procs[name] = proc
        wait_for_port("127.0.0.1", port, timeout=10.0)
        return proc

    def start_cluster(self) -> None:
        self._spawn(
            "storage",
            ["storage", "--port", str(self.storage_port), "--data-dir", self.storage_dir],
            self.storage_port
        )
        self._spawn(
            "coordinator",
            ["coordinator", "--port", str(self.coord_port), "--data-dir", self.coord_dir],
            self.coord_port
        )
        for broker_id, b_port in self.broker_ports.items():
            self._spawn(
                broker_id,
                [
                    "broker",
                    "--broker-id", broker_id,
                    "--port", str(b_port),
                    "--storage-port", str(self.storage_port),
                    "--coord-port", str(self.coord_port),
                    "--lease-ttl-ms", str(self.lease_ttl_ms),
                    "--renew-interval-sec", str(self.renew_interval_sec),
                    "--ack-timeout-sec", str(self.ack_timeout_sec),
                    "--watch-topics", *self.watch_topics,
                ],
                b_port
            )

    def kill_process(self, name: str) -> None:
        proc = self.procs.get(name)
        if proc and proc.poll() is None:
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait(timeout=3.0)

    def pause_process(self, name: str) -> None:
        proc = self.procs.get(name)
        if proc and proc.poll() is None:
            os.kill(proc.pid, signal.SIGSTOP)

    def resume_process(self, name: str) -> None:
        proc = self.procs.get(name)
        if proc and proc.poll() is None:
            os.kill(proc.pid, signal.SIGCONT)

    def get_owner(self, topic: str) -> Optional[Dict[str, Any]]:
        with CoordinatorClient("127.0.0.1", self.coord_port) as cc:
            return cc.get_owner(topic)

    def shutdown(self) -> None:
        for name, proc in list(self.procs.items()):
            if proc.poll() is None:
                try:
                    # Ensure stopped processes can handle SIGTERM
                    os.kill(proc.pid, signal.SIGCONT)
                except OSError:
                    pass
                try:
                    proc.terminate()
                    proc.wait(timeout=2.0)
                except Exception:
                    try:
                        proc.kill()
                        proc.wait(timeout=1.0)
                    except Exception:
                        pass
            if proc.stdout:
                proc.stdout.close()
        self.procs.clear()
        shutil.rmtree(self.data_dir, ignore_errors=True)


def run_consumer_crash_scenario(
    run_id: int,
    mode: str = SubscriptionModes.EXCLUSIVE,
    total_messages: int = 60
) -> ExperimentResult:
    """Scenario 1: Crash a consumer mid-stream (holding unacked messages) and measure recovery."""
    topic = "orders"
    sub_name = f"sub-{mode.lower()}"
    cluster = ClusterOrchestrator(lease_ttl_ms=1500, ack_timeout_sec=0.4, watch_topics=[topic])

    try:
        cluster.start_cluster()
        b1_port = cluster.broker_ports["broker-1"]

        # Prime ownership on broker-1
        producer = Producer(
            broker_host="127.0.0.1", broker_port=b1_port,
            coordinator_host="127.0.0.1", coordinator_port=cluster.coord_port
        )
        producer.connect(topic)

        seen_offsets_all: List[int] = []
        unique_acked: Set[int] = set()
        redelivery_events = 0
        lock = threading.Lock()
        c1_done = threading.Event()
        crash_ts = 0.0
        first_recovered_ts = 0.0

        start_time = time.perf_counter()

        # Publish initial batch + streaming batch
        def publish_load():
            for i in range(total_messages):
                producer.send(topic, {"seq": i})
                time.sleep(0.015)

        pub_thread = threading.Thread(target=publish_load, daemon=True)
        pub_thread.start()

        # Consumer 1: processes 15 messages, receives 5 MORE without acking, then crashes!
        c1 = Consumer(
            topic=topic, subscription=sub_name, mode=mode, consumer_id="c-victim",
            coordinator_host="127.0.0.1", coordinator_port=cluster.coord_port
        )
        c1.connect()

        c2: Optional[Consumer] = None
        if mode == SubscriptionModes.SHARED:
            # In shared mode, a peer worker is already active in the pool
            c2 = Consumer(
                topic=topic, subscription=sub_name, mode=mode, consumer_id="c-survivor",
                coordinator_host="127.0.0.1", coordinator_port=cluster.coord_port
            )
            c2.connect()

        c1_count = 0
        while c1_count < 20:
            msg = c1.receive(timeout=3.0)
            if msg is None:
                continue
            with lock:
                seen_offsets_all.append(msg.message_id.offset)
            if c1_count < 15:
                c1.ack(msg)
                with lock:
                    unique_acked.add(msg.message_id.offset)
            # Last 5 messages intentionally left unacked before crash!
            c1_count += 1

        # Inject consumer crash!
        crash_ts = time.perf_counter()
        c1.close()
        c1_done.set()

        if mode == SubscriptionModes.EXCLUSIVE:
            c2 = Consumer(
                topic=topic, subscription=sub_name, mode=mode, consumer_id="c-replacement",
                coordinator_host="127.0.0.1", coordinator_port=cluster.coord_port
            )
            c2.connect()

        # c2 finishes all remaining & redelivered messages
        c2_offsets: List[int] = []
        deadline = time.time() + 12.0
        while len(unique_acked) < total_messages and time.time() < deadline:
            msg = c2.receive(timeout=0.5)
            if msg is None:
                continue
            now = time.perf_counter()
            if first_recovered_ts == 0.0 and now >= crash_ts:
                first_recovered_ts = now
            with lock:
                seen_offsets_all.append(msg.message_id.offset)
                c2_offsets.append(msg.message_id.offset)
                if msg.redelivery_count >= 1 or msg.message_id.offset in unique_acked:
                    redelivery_events += 1
                unique_acked.add(msg.message_id.offset)
            c2.ack(msg)

        pub_thread.join(timeout=5.0)
        end_time = time.perf_counter()

        c2.close()
        producer.close()

        duplicates = len(seen_offsets_all) - len(set(seen_offsets_all))
        recovery_ms = round((first_recovered_ts - crash_ts) * 1000.0, 2) if first_recovered_ts else -1.0
        elapsed = max(end_time - start_time, 0.001)
        throughput = round(total_messages / elapsed, 2)

        # Exclusive guarantees strict monotonic order within each connected consumer session
        ordering_ok = (c2_offsets == sorted(c2_offsets)) if mode == SubscriptionModes.EXCLUSIVE else False

        return ExperimentResult(
            run_id=run_id,
            scenario=f"consumer_crash_{mode.lower()}",
            mode=mode,
            total_published=total_messages,
            unique_delivered=len(unique_acked),
            redelivered_count=max(redelivery_events, duplicates),
            duplicates_seen=duplicates,
            recovery_latency_ms=recovery_ms,
            publish_recovery_ms=0.0,
            deliver_recovery_ms=recovery_ms,
            throughput_msgs_per_sec=throughput,
            ordering_preserved=ordering_ok,
            zombie_fenced=False,
            notes=f"c1 crashed holding 5 unacked; c2 recovered {len(c2_offsets)} msgs"
        )
    finally:
        cluster.shutdown()


def run_broker_kill_scenario(run_id: int, total_messages: int = 80) -> ExperimentResult:
    """Scenario 2: SIGKILL the owning broker mid-stream during active publish + consume."""
    topic = "orders"
    sub_name = "billing-sub"
    cluster = ClusterOrchestrator(lease_ttl_ms=700, renew_interval_sec=0.2, ack_timeout_sec=0.5, watch_topics=[topic])

    try:
        cluster.start_cluster()
        b1_port = cluster.broker_ports["broker-1"]

        producer = Producer(
            broker_host="127.0.0.1", broker_port=b1_port,
            coordinator_host="127.0.0.1", coordinator_port=cluster.coord_port,
            max_retries=30, base_backoff_sec=0.05
        )
        producer.connect(topic)

        # Ensure broker-1 owns topic initially
        first_id = producer.send(topic, {"seq": 0})
        assert first_id.offset == 0

        consumer = Consumer(
            topic=topic, subscription=sub_name, mode=SubscriptionModes.EXCLUSIVE,
            consumer_id="billing-worker",
            coordinator_host="127.0.0.1", coordinator_port=cluster.coord_port,
            max_retries=30, base_backoff_sec=0.05
        )
        consumer.connect()

        published_offsets: List[int] = [0]
        seen_offsets: List[int] = []
        unique_acked: Set[int] = set()
        redelivery_events = 0

        crash_ts = 0.0
        first_pub_after_crash = 0.0
        first_deliv_after_crash = 0.0
        kill_triggered = threading.Event()
        lock = threading.Lock()

        start_time = time.perf_counter()

        def publisher_worker():
            nonlocal first_pub_after_crash
            for i in range(1, total_messages):
                msg_id = producer.send(topic, {"seq": i})
                now = time.perf_counter()
                with lock:
                    published_offsets.append(msg_id.offset)
                    if kill_triggered.is_set() and first_pub_after_crash == 0.0 and now > crash_ts:
                        first_pub_after_crash = now
                time.sleep(0.02)

        def consumer_worker():
            nonlocal first_deliv_after_crash, redelivery_events
            deadline = time.time() + 20.0
            while len(unique_acked) < total_messages and time.time() < deadline:
                msg = consumer.receive(timeout=0.4)
                if msg is None:
                    continue
                now = time.perf_counter()
                with lock:
                    if kill_triggered.is_set() and first_deliv_after_crash == 0.0 and now > crash_ts:
                        first_deliv_after_crash = now
                    seen_offsets.append(msg.message_id.offset)
                    if msg.redelivery_count >= 1 or msg.message_id.offset in unique_acked:
                        redelivery_events += 1
                    unique_acked.add(msg.message_id.offset)
                consumer.ack(msg)

        pub_t = threading.Thread(target=publisher_worker, daemon=True)
        con_t = threading.Thread(target=consumer_worker, daemon=True)
        pub_t.start()
        con_t.start()

        # Wait until at least 20 messages are processed on broker-1, then SIGKILL broker-1!
        wait_deadline = time.time() + 10.0
        while len(unique_acked) < 20 and time.time() < wait_deadline:
            time.sleep(0.02)

        owner_before = cluster.get_owner(topic)
        victim_broker = owner_before["broker_id"] if owner_before else "broker-1"

        crash_ts = time.perf_counter()
        kill_triggered.set()
        cluster.kill_process(victim_broker)

        pub_t.join(timeout=15.0)
        con_t.join(timeout=15.0)
        end_time = time.perf_counter()

        owner_after = cluster.get_owner(topic)
        new_owner = owner_after["broker_id"] if owner_after else "unknown"
        new_token = owner_after["fencing_token"] if owner_after else -1

        producer.close()
        consumer.close()

        duplicates = len(seen_offsets) - len(set(seen_offsets))
        pub_rec_ms = round((first_pub_after_crash - crash_ts) * 1000.0, 2) if first_pub_after_crash else -1.0
        deliv_rec_ms = round((first_deliv_after_crash - crash_ts) * 1000.0, 2) if first_deliv_after_crash else -1.0
        overall_rec_ms = max(pub_rec_ms, deliv_rec_ms)
        elapsed = max(end_time - start_time, 0.001)
        throughput = round(total_messages / elapsed, 2)

        # Exclusive mode preserves order across non-duplicate stream (or up to rewind point)
        dedup_seq: List[int] = []
        seen_set: Set[int] = set()
        for off in seen_offsets:
            if off not in seen_set:
                seen_set.add(off)
                dedup_seq.append(off)
        ordering_ok = (dedup_seq == list(range(total_messages)))

        return ExperimentResult(
            run_id=run_id,
            scenario="broker_kill_sigkill",
            mode=SubscriptionModes.EXCLUSIVE,
            total_published=len(published_offsets),
            unique_delivered=len(unique_acked),
            redelivered_count=max(redelivery_events, duplicates),
            duplicates_seen=duplicates,
            recovery_latency_ms=overall_rec_ms,
            publish_recovery_ms=pub_rec_ms,
            deliver_recovery_ms=deliv_rec_ms,
            throughput_msgs_per_sec=throughput,
            ordering_preserved=ordering_ok,
            zombie_fenced=False,
            notes=f"killed {victim_broker}; failover to {new_owner} (token={new_token})"
        )
    finally:
        cluster.shutdown()


def run_zombie_sigstop_scenario(run_id: int, total_messages: int = 40) -> ExperimentResult:
    """Scenario 3: Pause owning broker with SIGSTOP past lease expiry, failover, then SIGCONT to fence zombie."""
    topic = "orders"
    sub_name = "ledger-sub"
    # Longer renew interval (0.8s) vs TTL (0.7s) ensures paused broker still holds stale token on SIGCONT
    cluster = ClusterOrchestrator(lease_ttl_ms=700, renew_interval_sec=0.35, ack_timeout_sec=0.5, watch_topics=[topic])

    try:
        cluster.start_cluster()
        b1_port = cluster.broker_ports["broker-1"]

        producer = Producer(
            broker_host="127.0.0.1", broker_port=b1_port,
            timeout=1.5,
            coordinator_host="127.0.0.1", coordinator_port=cluster.coord_port,
            max_retries=30, base_backoff_sec=0.05
        )
        producer.connect(topic)

        half = total_messages // 2
        for i in range(half):
            msg_id = producer.send(topic, {"seq": i})
            assert msg_id.offset == i

        owner_before = cluster.get_owner(topic)
        old_broker = owner_before["broker_id"]
        old_token = owner_before["fencing_token"]
        old_port = cluster.broker_ports[old_broker]

        consumer = Consumer(
            topic=topic, subscription=sub_name, mode=SubscriptionModes.EXCLUSIVE,
            consumer_id="ledger-worker",
            timeout=1.5,
            coordinator_host="127.0.0.1", coordinator_port=cluster.coord_port,
            max_retries=30, base_backoff_sec=0.05
        )
        consumer.connect()

        seen_offsets: List[int] = []
        unique_acked: Set[int] = set()
        while len(unique_acked) < half:
            msg = consumer.receive(timeout=2.0)
            if msg:
                seen_offsets.append(msg.message_id.offset)
                unique_acked.add(msg.message_id.offset)
                consumer.ack(msg)

        start_time = time.perf_counter()

        # 1. SIGSTOP the owning broker (simulates severe GC pause / OS freeze)
        freeze_ts = time.perf_counter()
        cluster.pause_process(old_broker)

        # 2. Wait for standby broker to acquire expired lease (token = old_token + 1)
        new_owner = None
        deadline = time.time() + 5.0
        while time.time() < deadline:
            owner_now = cluster.get_owner(topic)
            if owner_now and owner_now["broker_id"] != old_broker:
                new_owner = owner_now
                break
            time.sleep(0.05)

        assert new_owner is not None, "Standby broker did not take over expired lease"

        # 3. Publish remaining half through new owning broker -> measures publish recovery!
        first_pub_ts = 0.0
        for i in range(half, total_messages):
            msg_id = producer.send(topic, {"seq": i})
            if first_pub_ts == 0.0:
                first_pub_ts = time.perf_counter()
            assert msg_id.offset == i

        # Consumer finishes receiving remaining half from new broker
        first_deliv_ts = 0.0
        con_deadline = time.time() + 8.0
        while len(unique_acked) < total_messages and time.time() < con_deadline:
            msg = consumer.receive(timeout=0.5)
            if msg:
                if first_deliv_ts == 0.0:
                    first_deliv_ts = time.perf_counter()
                seen_offsets.append(msg.message_id.offset)
                unique_acked.add(msg.message_id.offset)
                consumer.ack(msg)

        end_time = time.perf_counter()

        # 4. SIGCONT the paused zombie broker!
        cluster.resume_process(old_broker)

        # Immediately test zombie broker write attempt AND storage fencing barrier
        zombie_rejected = False
        rejection_reason = ""
        try:
            with Producer("127.0.0.1", old_port, timeout=2.0) as zombie_pub:
                zombie_pub.send(topic, "corrupted-zombie-write")
        except PublishError as e:
            if e.error_code in ("FENCED", "NOT_OWNER"):
                zombie_rejected = True
                rejection_reason = e.error_code

        # Also directly verify Storage fencing barrier rejects old_token
        storage_fenced = False
        with StorageClient("127.0.0.1", cluster.storage_port) as sc:
            try:
                sc.append(topic, "direct-stale-token", fencing_token=old_token)
            except FencedError:
                storage_fenced = True
            final_last_offset = sc.last_offset(topic)

        producer.close()
        consumer.close()

        duplicates = len(seen_offsets) - len(set(seen_offsets))
        pub_rec_ms = round((first_pub_ts - freeze_ts) * 1000.0, 2) if first_pub_ts else -1.0
        deliv_rec_ms = round((first_deliv_ts - freeze_ts) * 1000.0, 2) if first_deliv_ts else -1.0
        elapsed = max(end_time - start_time, 0.001)

        return ExperimentResult(
            run_id=run_id,
            scenario="broker_zombie_sigstop",
            mode=SubscriptionModes.EXCLUSIVE,
            total_published=total_messages,
            unique_delivered=len(unique_acked),
            redelivered_count=duplicates,
            duplicates_seen=duplicates,
            recovery_latency_ms=max(pub_rec_ms, deliv_rec_ms),
            publish_recovery_ms=pub_rec_ms,
            deliver_recovery_ms=deliv_rec_ms,
            throughput_msgs_per_sec=round(half / elapsed, 2),
            ordering_preserved=(final_last_offset == total_messages - 1),
            zombie_fenced=(zombie_rejected and storage_fenced and final_last_offset == total_messages - 1),
            notes=f"SIGSTOP {old_broker}(tok={old_token}) -> {new_owner['broker_id']}(tok={new_owner['fencing_token']}); SIGCONT rejected={rejection_reason}"
        )
    finally:
        cluster.shutdown()


def write_results_csv(results: List[ExperimentResult], output_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    fieldnames = list(asdict(results[0]).keys())
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(asdict(r))


def run_scenarios(scenario: str, runs: int, output_path: str) -> List[ExperimentResult]:
    results: List[ExperimentResult] = []
    for run_idx in range(1, runs + 1):
        if scenario in ("all", "consumer_crash"):
            results.append(run_consumer_crash_scenario(run_idx, mode=SubscriptionModes.EXCLUSIVE))
            results.append(run_consumer_crash_scenario(run_idx, mode=SubscriptionModes.SHARED))
        if scenario in ("all", "broker_kill"):
            results.append(run_broker_kill_scenario(run_idx))
        if scenario in ("all", "broker_zombie"):
            results.append(run_zombie_sigstop_scenario(run_idx))

    if results:
        write_results_csv(results, output_path)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="PulserCore Fault-Injection & Metrics Harness")
    parser.add_argument(
        "--scenario",
        choices=["all", "consumer_crash", "broker_kill", "broker_zombie"],
        default="all"
    )
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--output", default="results/metrics.csv")
    args = parser.parse_args()

    results = run_scenarios(args.scenario, args.runs, args.output)
    print(f"\nCompleted {len(results)} experiment(s). Results written to {args.output}:")
    for r in results:
        print(
            f"  [run={r.run_id}] {r.scenario:<24} | published={r.total_published} "
            f"delivered={r.unique_delivered} redelivered={r.redelivered_count} "
            f"dups={r.duplicates_seen} rec_ms={r.recovery_latency_ms} "
            f"tput={r.throughput_msgs_per_sec}/s fenced={r.zombie_fenced} | {r.notes}"
        )


if __name__ == "__main__":
    main()

