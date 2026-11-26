"""Phase 7 Validation Tests: UnackedTracker, Timeout Redelivery & Integration Checkpoint B."""

import shutil
import tempfile
import time
import pytest

from broker.broker_server import BrokerServer
from broker.unacked_tracker import UnackedTracker
from client.consumer import Consumer
from client.producer import Producer
from common.protocol import MessageId, SubscriptionModes
from storage.storage_server import StorageServer


@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp(prefix="pulser_test_phase7_")
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


class TestUnackedTrackerUnit:
    """Unit test the min-heap priority queue tracking logic in complete isolation."""

    def test_tracking_and_lazy_ack_deletion(self):
        tracker = UnackedTracker(default_timeout_sec=1.0)

        m0 = MessageId("topic", 0)
        m1 = MessageId("topic", 1)
        m2 = MessageId("topic", 2)

        # Track 3 messages with artificial deadlines
        now = 1000.0
        tracker.track(m0, "c1", timeout_sec=1.0, now=now)   # Deadline: 1001.0
        tracker.track(m1, "c1", timeout_sec=2.0, now=now)   # Deadline: 1002.0
        tracker.track(m2, "c2", timeout_sec=3.0, now=now)   # Deadline: 1003.0

        # Consumer acks m1 before it expires
        acked = tracker.ack(m1)
        assert acked is True

        # Check at 1001.5 (m0 should expire, m1 was acked, m2 still active)
        exp_1 = tracker.expired(now=1001.5)
        assert len(exp_1) == 1
        assert exp_1[0].msg_id == m0
        assert exp_1[0].redelivery_count == 1

        # Check at 1002.5 (m1 was acked, should not fire!)
        exp_2 = tracker.expired(now=1002.5)
        assert len(exp_2) == 0

        # Check at 1003.5 (m2 expires)
        exp_3 = tracker.expired(now=1003.5)
        assert len(exp_3) == 1
        assert exp_3[0].msg_id == m2
        assert exp_3[0].redelivery_count == 1


class TestPhase7DoneWhenCriterion:
    """Official Phase 7 Done When Criterion:
    A consumer that never acks gets its messages redelivered after the timeout.
    """

    def test_consumer_never_acks_triggers_timeout_redelivery(self, temp_dir):
        storage_server = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="Storage")
        storage_port = storage_server.start()

        # Broker configured with short 0.4s ack timeout
        broker_server = BrokerServer(
            host="127.0.0.1",
            port=0,
            storage_host="127.0.0.1",
            storage_port=storage_port,
            broker_id="broker-1",
            ack_timeout_sec=0.4,
            name="Broker-1"
        )
        broker_port = broker_server.start()

        topic = "orders"
        sub_name = "timeout-sub"

        try:
            # 1. Publish 1 message
            with Producer(broker_host="127.0.0.1", broker_port=broker_port) as producer:
                producer.send(topic, "vital-order")

            # 2. Consumer connects
            with Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name) as consumer:
                # First delivery
                m1 = consumer.receive(timeout=2.0)
                assert m1 is not None
                assert m1.message_id.offset == 0
                assert m1.payload == "vital-order"
                assert m1.redelivery_count == 0

                # 3. Consumer intentionally DOES NOT ACK!
                # Wait past the 0.4s timeout
                time.sleep(0.5)

                # 4. Message must be automatically redelivered!
                m2 = consumer.receive(timeout=2.0)
                assert m2 is not None
                assert m2.message_id.offset == 0
                assert m2.payload == "vital-order"
                assert m2.redelivery_count >= 1

                # Finally ack to complete processing
                consumer.ack(m2)
        finally:
            broker_server.stop()
            storage_server.stop()


class TestIntegrationCheckpointB:
    """Official Integration Checkpoint B:
    Both subscription modes work end to end with acks and timeout redelivery.
    """

    def test_shared_mode_timeout_redelivers_to_alternate_worker(self, temp_dir):
        storage_server = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="Storage")
        storage_port = storage_server.start()

        broker_server = BrokerServer(
            host="127.0.0.1",
            port=0,
            storage_host="127.0.0.1",
            storage_port=storage_port,
            broker_id="broker-1",
            ack_timeout_sec=0.4,
            name="Broker-1"
        )
        broker_port = broker_server.start()

        topic = "shared-jobs"
        sub_name = "worker-group"

        try:
            # Connect Worker 1 and Worker 2 in SHARED mode
            c1 = Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name, mode=SubscriptionModes.SHARED, consumer_id="slow-worker")
            c2 = Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name, mode=SubscriptionModes.SHARED, consumer_id="healthy-worker")
            c1.connect()
            c2.connect()

            # Publish a message
            with Producer(broker_host="127.0.0.1", broker_port=broker_port) as producer:
                producer.send(topic, "critical-job")

            # One worker gets the message
            received_1 = c1.receive(timeout=2.0)
            target_worker = c1 if received_1 else c2
            other_worker = c2 if received_1 else c1
            msg = received_1 or other_worker.receive(timeout=2.0)
            assert msg is not None

            # Simulating a freeze: target_worker DOES NOT ACK!
            # Wait for timeout to expire (> 0.4s)
            time.sleep(0.5)

            # The message is automatically pulled back and delivered to other_worker!
            redelivered = other_worker.receive(timeout=2.0)
            assert redelivered is not None
            assert redelivered.message_id.offset == msg.message_id.offset
            assert redelivered.redelivery_count >= 1

            # Healthy worker acks the redelivery
            other_worker.ack(redelivered)

            c1.close()
            c2.close()
        finally:
            broker_server.stop()
            storage_server.stop()
