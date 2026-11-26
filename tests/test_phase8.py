"""Phase 8 Validation Tests: Retention Rules (Time-based, Size-based & Multi-Subscription Safety)."""

import shutil
import tempfile
import pytest

from common.cursor import Cursor
from storage.log_manager import LogManager
from storage.storage_client import StorageClient
from storage.storage_server import StorageServer


@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp(prefix="pulser_test_phase8_")
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


class TestRetentionRules:
    """Test time-based, size-based, and multi-subscription retention invariants."""

    def test_time_based_retention_keeps_unacked_messages(self, temp_dir):
        """Phase 8 Done When Criterion:
        Old, fully acked data is removed while unacked data is kept.
        """
        manager = LogManager(temp_dir)
        topic = "orders"
        try:
            # 1. Append 10 old messages (t = 1000) and 5 recent messages (t = 5000)
            for i in range(10):
                manager.append(topic, f"old-order-{i}", fencing_token=1, timestamp=1000.0)
            for i in range(10, 15):
                manager.append(topic, f"new-order-{i}", fencing_token=1, timestamp=5000.0)

            # 2. Fast subscription acked up to offset 12; slow subscription acked only up to offset 5
            manager.save_cursor(topic, "fast-sub", Cursor(mark_delete_position=12), fencing_token=1)
            manager.save_cursor(topic, "slow-sub", Cursor(mark_delete_position=5), fencing_token=1)

            # 3. Apply 1-hour (3600s) time retention at t = 5000.0
            # Offsets 0..9 are 4000s old (past retention).
            # Offsets 0..5 are acked by BOTH fast-sub and slow-sub.
            # Offsets 6..9 are old, but NOT acked by slow-sub -> MUST BE KEPT!
            stats = manager.apply_retention(topic, retention_time_sec=3600.0, now=5000.0)

            assert stats["safe_acked_upto"] == 5
            assert stats["deleted_count"] == 6
            assert stats["new_base_offset"] == 6
            assert stats["remaining_count"] == 9
            assert manager.base_offset(topic) == 6
            assert manager.last_offset(topic) == 14

            # 4. Verify reading from offset 0 clamps cleanly to base_offset 6 and returns 6..14
            msgs, next_off = manager.read(topic, from_offset=0, max_count=50)
            assert [m["offset"] for m in msgs] == list(range(6, 15))
            assert next_off == 15

            # 5. Now slow-sub catches up to offset 14
            manager.save_cursor(topic, "slow-sub", Cursor(mark_delete_position=14), fencing_token=1)
            stats2 = manager.apply_retention(topic, retention_time_sec=3600.0, now=5000.0)

            # Offsets 6..9 are now acked by all AND past retention -> deleted!
            # Offsets 10..14 are acked by fast-sub (up to 12), but are 0s old (not past retention) -> KEPT!
            assert stats2["deleted_count"] == 4
            assert stats2["new_base_offset"] == 10
            assert stats2["remaining_count"] == 5

            msgs2, _ = manager.read(topic, from_offset=10, max_count=20)
            assert [m["offset"] for m in msgs2] == [10, 11, 12, 13, 14]
        finally:
            manager.close()

    def test_size_based_retention_respects_unacked_boundary(self, temp_dir):
        """Verify size-based retention deletes oldest acked data to stay under byte limit,
        without ever deleting unacknowledged data.
        """
        manager = LogManager(temp_dir)
        topic = "telemetry"
        try:
            for i in range(10):
                manager.append(topic, f"payload-{i}-" + ("x" * 50), fencing_token=1, timestamp=1000.0)

            # Only offsets 0..2 are acked
            manager.save_cursor(topic, "worker-sub", Cursor(mark_delete_position=2), fencing_token=1)

            # Size limit of 100 bytes would normally delete everything, but unacked 3..9 MUST survive!
            stats1 = manager.apply_retention(topic, retention_size_bytes=100)
            assert stats1["deleted_count"] == 3
            assert stats1["new_base_offset"] == 3
            assert stats1["remaining_count"] == 7

            # Now ack up to offset 9, and apply size limit of 350 bytes
            manager.save_cursor(topic, "worker-sub", Cursor(mark_delete_position=9), fencing_token=1)
            stats2 = manager.apply_retention(topic, retention_size_bytes=350)
            assert stats2["deleted_count"] > 0
            assert stats2["remaining_bytes"] <= 350
            assert stats2["remaining_count"] >= 1
        finally:
            manager.close()

    def test_offset_monotonicity_after_complete_truncation_and_restart(self, temp_dir):
        """If retention deletes ALL messages on disk, offsets must NEVER reset to 0 on restart."""
        topic = "ephemeral-events"
        m1 = LogManager(temp_dir)
        for i in range(5):
            m1.append(topic, f"evt-{i}", fencing_token=1, timestamp=100.0)
        m1.save_cursor(topic, "sub-a", Cursor(mark_delete_position=4), fencing_token=1)

        stats = m1.apply_retention(topic, retention_time_sec=60.0, now=1000.0)
        assert stats["deleted_count"] == 5
        assert stats["remaining_count"] == 0
        assert stats["new_base_offset"] == 5
        m1.close()

        # Restart LogManager from disk
        m2 = LogManager(temp_dir)
        try:
            assert m2.last_offset(topic) == 4
            assert m2.base_offset(topic) == 5
            next_off = m2.append(topic, "evt-5", fencing_token=1, timestamp=1000.0)
            assert next_off == 5
            msgs, _ = m2.read(topic, from_offset=5, max_count=10)
            assert len(msgs) == 1
            assert msgs[0]["payload"] == "evt-5"
        finally:
            m2.close()


class TestRetentionOverTCP:
    """Verify retention configuration and execution over StorageServer/StorageClient."""

    def test_network_set_and_apply_retention(self, temp_dir):
        server = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="RetentionStorage")
        port = server.start()

        try:
            with StorageClient("127.0.0.1", port) as client:
                topic = "logs"
                # Prime 5 messages via log_manager with old timestamps
                for i in range(5):
                    server.log_manager.append(topic, f"log-{i}", fencing_token=1, timestamp=100.0)
                client.append(topic, "log-5", fencing_token=1)

                # Ack only 0..3
                client.save_cursor(topic, "archiver", Cursor(mark_delete_position=3), fencing_token=1)
                client.set_retention(topic, retention_time_sec=60.0)

                stats = client.apply_retention(topic)
                assert stats["deleted_count"] == 4
                assert stats["new_base_offset"] == 4
                assert stats["remaining_count"] == 2

                msgs, _ = client.read(topic, from_offset=0, max_count=10)
                assert [m["offset"] for m in msgs] == [4, 5]
        finally:
            server.stop()
