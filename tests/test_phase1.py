"""Phase 1 Validation Tests: Storage Service, Append-Only Log, fsync Durability & Fencing."""

import os
import shutil
import tempfile
import pytest

from common.protocol import ErrorCodes
from storage.log_manager import FencedError, LogManager
from storage.storage_client import StorageClient
from storage.storage_server import StorageServer


@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp(prefix="pulser_test_storage_")
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


class TestLogManagerBasic:
    """Test core append-only disk logging and index mechanics."""

    def test_append_monotonic_offsets(self, temp_dir):
        manager = LogManager(temp_dir)
        try:
            assert manager.last_offset("orders") == -1

            off0 = manager.append("orders", "order-1", fencing_token=1)
            off1 = manager.append("orders", "order-2", fencing_token=1)
            off2 = manager.append("orders", "order-3", fencing_token=1)

            assert off0 == 0
            assert off1 == 1
            assert off2 == 2
            assert manager.last_offset("orders") == 2
        finally:
            manager.close()

    def test_read_pagination(self, temp_dir):
        manager = LogManager(temp_dir)
        try:
            for i in range(10):
                manager.append("events", f"event-{i}", fencing_token=1)

            # Read first 5
            msgs, next_off = manager.read("events", from_offset=0, max_count=5)
            assert len(msgs) == 5
            assert msgs[0]["payload"] == "event-0"
            assert msgs[4]["payload"] == "event-4"
            assert next_off == 5

            # Read remaining
            msgs_tail, next_tail = manager.read("events", from_offset=5, max_count=10)
            assert len(msgs_tail) == 5
            assert msgs_tail[0]["payload"] == "event-5"
            assert msgs_tail[-1]["payload"] == "event-9"
            assert next_tail == 10

            # Read past end
            empty, next_empty = manager.read("events", from_offset=10, max_count=5)
            assert empty == []
            assert next_empty == 10
        finally:
            manager.close()


class TestFencingEnforcement:
    """Verify that storage rejects writes with stale fencing tokens."""

    def test_fencing_rejects_zombie_token(self, temp_dir):
        manager = LogManager(temp_dir)
        try:
            # 1. Broker 1 appends with token 1
            manager.append("payments", "pay-1", fencing_token=1)

            # 2. Broker 2 takes over with token 2
            manager.append("payments", "pay-2", fencing_token=2)
            assert manager.get_highest_token("payments") == 2

            # 3. Zombie Broker 1 wakes up and attempts write with token 1
            with pytest.raises(FencedError) as exc_info:
                manager.append("payments", "zombie-pay", fencing_token=1)

            assert "Fenced" in str(exc_info.value)
            assert exc_info.value.current_token == 2
            assert exc_info.value.attempted_token == 1

            # 4. Broker 2 can still write with token 2
            off3 = manager.append("payments", "pay-3", fencing_token=2)
            assert off3 == 2
        finally:
            manager.close()


class TestStorageNetworkServer:
    """Test storage service over real TCP sockets via StorageClient."""

    def test_network_append_read_and_fencing(self, temp_dir):
        server = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="TestStorage")
        port = server.start()

        try:
            with StorageClient(host="127.0.0.1", port=port) as client:
                # Append
                off0 = client.append("metrics", "cpu=12%", fencing_token=5)
                off1 = client.append("metrics", "cpu=15%", fencing_token=5)
                assert off0 == 0
                assert off1 == 1

                # Last offset
                assert client.last_offset("metrics") == 1

                # Read
                msgs, next_off = client.read("metrics", from_offset=0, max_count=10)
                assert len(msgs) == 2
                assert msgs[0]["payload"] == "cpu=12%"
                assert next_off == 2

                # Advance token to 10
                client.append("metrics", "cpu=18%", fencing_token=10)

                # Stale write attempt (token 5 < 10)
                with pytest.raises(FencedError):
                    client.append("metrics", "cpu=99%", fencing_token=5)
        finally:
            server.stop()


class TestPhase1CrashAndRecoveryCriterion:
    """Official Phase 1 Done When Criterion:
    Append 1,000 messages, kill storage, restart it and read all 1,000 back;
    a write with a stale token gets FENCED.
    """

    def test_append_1000_kill_restart_and_fence(self, temp_dir):
        topic = "critical-transactions"

        # Step 1: Start storage server and append 1,000 messages with fsync
        server1 = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="Storage-1")
        port1 = server1.start()

        with StorageClient(host="127.0.0.1", port=port1) as client:
            for i in range(1000):
                off = client.append(topic, f"tx-{i}", fencing_token=100)
                assert off == i

            assert client.last_offset(topic) == 999

        # Step 2: Simulate crash (kill storage abruptly without saving in-memory state)
        server1.stop()

        # Step 3: Start a brand-new storage service pointing to the identical disk directory
        server2 = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="Storage-2")
        port2 = server2.start()

        try:
            with StorageClient(host="127.0.0.1", port=port2) as client2:
                # Verify last offset was restored
                assert client2.last_offset(topic) == 999

                # Read all 1,000 messages back in chunks of 200
                total_read = 0
                while total_read < 1000:
                    msgs, next_off = client2.read(topic, from_offset=total_read, max_count=200)
                    assert len(msgs) > 0
                    for idx, msg in enumerate(msgs):
                        expected_offset = total_read + idx
                        assert msg["offset"] == expected_offset
                        assert msg["payload"] == f"tx-{expected_offset}"
                    total_read = next_off

                assert total_read == 1000

                # Step 4: Verify fencing state survived crash:
                # Highest token was 100. Attempting write with token 99 must get FENCED.
                with pytest.raises(FencedError):
                    client2.append(topic, "stale-tx", fencing_token=99)

                # A valid token write (e.g. 101) succeeds and receives offset 1000
                new_off = client2.append(topic, "fresh-tx", fencing_token=101)
                assert new_off == 1000
        finally:
            server2.stop()

