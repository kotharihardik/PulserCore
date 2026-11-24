"""Phase 4 Validation Tests: Durable Subscription Cursors, Compaction & Topic Metadata."""

import os
import shutil
import tempfile
import pytest

from common.cursor import Cursor
from common.protocol import ErrorCodes
from storage.log_manager import FencedError, LogManager
from storage.storage_client import StorageClient
from storage.storage_server import StorageServer


@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp(prefix="pulser_test_phase4_")
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


class TestCursorLogic:
    """Test mark-delete positions, out-of-order individual acks, and automatic compaction."""

    def test_sequential_acks_advance_mark_delete(self):
        cursor = Cursor()
        assert cursor.mark_delete_position == -1
        assert len(cursor.individual_acks) == 0

        cursor.ack(0)
        assert cursor.mark_delete_position == 0
        assert len(cursor.individual_acks) == 0

        cursor.ack(1)
        cursor.ack(2)
        assert cursor.mark_delete_position == 2
        assert len(cursor.individual_acks) == 0

    def test_out_of_order_acks_and_automatic_compaction(self):
        """Simulate worker pool in Shared subscription acking out-of-order."""
        cursor = Cursor(mark_delete_position=2)

        # Workers complete offsets 4 and 5 while offset 3 is still in progress
        cursor.ack(4)
        cursor.ack(5)

        # Mark-delete cannot advance past 2 because 3 is missing!
        assert cursor.mark_delete_position == 2
        assert cursor.individual_acks == {4, 5}

        # Another worker completes offset 7
        cursor.ack(7)
        assert cursor.mark_delete_position == 2
        assert cursor.individual_acks == {4, 5, 7}

        # NOW worker processing offset 3 finally finishes and acks 3!
        cursor.ack(3)

        # Automatic compaction must occur:
        # Contiguous range is now 0..5! mark-delete position leaps to 5, and 4 & 5 are removed!
        assert cursor.mark_delete_position == 5
        assert cursor.individual_acks == {7}

    def test_is_acked_queries(self):
        cursor = Cursor(mark_delete_position=10, individual_acks={12, 15})
        assert cursor.is_acked(5) is True    # Below mark-delete
        assert cursor.is_acked(10) is True   # At mark-delete
        assert cursor.is_acked(11) is False  # Missing gap
        assert cursor.is_acked(12) is True   # In individual acks
        assert cursor.is_acked(13) is False  # Missing gap
        assert cursor.is_acked(15) is True   # In individual acks
        assert cursor.is_acked(16) is False  # Beyond all acks


class TestTopicMetadataPersistence:
    """Test topic-level metadata persistence (retention rules & subscriptions)."""

    def test_save_and_retrieve_metadata(self, temp_dir):
        manager = LogManager(temp_dir)
        try:
            meta = {
                "retention_time_sec": 3600,
                "retention_size_bytes": 1024 * 1024 * 50,
                "subscriptions": ["billing-sub", "analytics-sub"]
            }
            manager.save_topic_metadata("orders", meta)

            loaded = manager.get_topic_metadata("orders")
            assert loaded["topic"] == "orders"
            assert loaded["retention_time_sec"] == 3600
            assert "billing-sub" in loaded["subscriptions"]
        finally:
            manager.close()


class TestPhase4DoneWhenCriterion:
    """Official Phase 4 Done When Criterion:
    Save a cursor, restart storage, reload the identical cursor.
    """

    def test_save_cursor_restart_storage_and_reload_identical(self, temp_dir):
        topic = "financial-ledger"
        subscription = "audit-service"

        # 1. Start Storage Server 1
        server1 = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="Storage-1")
        port1 = server1.start()

        cursor_original = Cursor(mark_delete_position=150, individual_acks={152, 154, 160})

        with StorageClient(host="127.0.0.1", port=port1) as client1:
            client1.save_cursor(topic, subscription, cursor_original, fencing_token=10)

        # 2. Kill Storage Server 1 abruptly
        server1.stop()

        # 3. Start Storage Server 2 pointing to the same data directory
        server2 = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="Storage-2")
        port2 = server2.start()

        try:
            with StorageClient(host="127.0.0.1", port=port2) as client2:
                # 4. Reload cursor and verify it is 100% identical!
                cursor_reloaded = client2.load_cursor(topic, subscription)

                assert cursor_reloaded.mark_delete_position == cursor_original.mark_delete_position
                assert cursor_reloaded.individual_acks == cursor_original.individual_acks

                # 5. Verify fencing check survives restart:
                # Stale token 9 must get FENCED (highest seen was 10)
                stale_cursor = Cursor(mark_delete_position=151)
                with pytest.raises(FencedError):
                    client2.save_cursor(topic, subscription, stale_cursor, fencing_token=9)

                # Valid token 10 or higher succeeds
                fresh_cursor = Cursor(mark_delete_position=151, individual_acks={154, 160})
                client2.save_cursor(topic, subscription, fresh_cursor, fencing_token=10)

                reloaded_again = client2.load_cursor(topic, subscription)
                assert reloaded_again.mark_delete_position == 151
                assert reloaded_again.individual_acks == {154, 160}
        finally:
            server2.stop()

