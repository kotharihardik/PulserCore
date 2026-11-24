"""Phase 6 Validation Tests: Shared Subscriptions, Round-Robin Load Distribution & Re-pooling."""

import shutil
import tempfile
import threading
import time
import pytest

from broker.broker_server import BrokerServer
from client.consumer import Consumer
from client.producer import Producer
from common.protocol import SubscriptionModes
from storage.storage_server import StorageServer


@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp(prefix="pulser_test_phase6_")
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


class TestSharedSubscription:
    """Test multi-consumer attachments and disconnect re-pooling."""

    def test_multiple_consumers_can_join_shared_mode(self, temp_dir):
        storage_server = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="Storage")
        storage_port = storage_server.start()

        broker_server = BrokerServer(
            host="127.0.0.1",
            port=0,
            storage_host="127.0.0.1",
            storage_port=storage_port,
            broker_id="broker-1",
            name="Broker-1"
        )
        broker_port = broker_server.start()

        topic = "tasks"
        sub_name = "shared-workers"

        try:
            # 3 consumers can connect simultaneously to the SAME subscription name
            c1 = Consumer(host="127.0.0.1", broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name, mode=SubscriptionModes.SHARED, consumer_id="worker-1") if False else Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name, mode=SubscriptionModes.SHARED, consumer_id="worker-1")
            c2 = Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name, mode=SubscriptionModes.SHARED, consumer_id="worker-2")
            c3 = Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name, mode=SubscriptionModes.SHARED, consumer_id="worker-3")

            c1.connect()
            c2.connect()
            c3.connect()

            time.sleep(0.1)
            # All 3 are active
            assert c1._is_subscribed is True
            assert c2._is_subscribed is True
            assert c3._is_subscribed is True

            c1.close()
            c2.close()
            c3.close()
        finally:
            broker_server.stop()
            storage_server.stop()

    def test_consumer_disconnect_requeues_unacked_messages(self, temp_dir):
        """When a worker dies without acking, its messages are returned to the pool for others."""
        storage_server = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="Storage")
        storage_port = storage_server.start()

        broker_server = BrokerServer(
            host="127.0.0.1",
            port=0,
            storage_host="127.0.0.1",
            storage_port=storage_port,
            broker_id="broker-1",
            name="Broker-1"
        )
        broker_port = broker_server.start()

        topic = "orders"
        sub_name = "resilient-pool"

        try:
            # Connect Worker 1 and Worker 2
            c1 = Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name, mode=SubscriptionModes.SHARED, consumer_id="w-1")
            c2 = Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name, mode=SubscriptionModes.SHARED, consumer_id="w-2")
            c1.connect()
            c2.connect()

            # Publish 4 messages
            with Producer(broker_host="127.0.0.1", broker_port=broker_port) as p:
                for i in range(4):
                    p.send(topic, f"msg-{i}")

            # Each worker receives messages
            m1 = c1.receive(timeout=2.0)
            m2 = c2.receive(timeout=2.0)
            assert m1 is not None
            assert m2 is not None

            # Worker 1 acks its first message (offset 0)
            c1.ack(m1)

            # Worker 2 crashes/disconnects WITHOUT acking m2!
            # Worker 2 held unacked messages (offset 1 and offset 3)
            c2.close()
            time.sleep(0.15)

            # Worker 1 consumes remaining messages until all 4 are received
            remaining_received = []
            for _ in range(3):
                msg = c1.receive(timeout=3.0)
                if msg:
                    remaining_received.append(msg)
                    c1.ack(msg)

            # Worker 1 must receive the remaining 3 messages: offsets 1, 2, and 3
            received_offsets = {msg.message_id.offset for msg in remaining_received}
            assert received_offsets == {1, 2, 3}

            # Check that m2's offset (1) was indeed redelivered with redelivery_count >= 1
            requeued_m2 = next(m for m in remaining_received if m.message_id.offset == m2.message_id.offset)
            assert requeued_m2.redelivery_count >= 1

            c1.close()
        finally:
            broker_server.stop()
            storage_server.stop()


class TestPhase6DoneWhenCriterion:
    """Official Phase 6 Done When Criterion:
    With 3 consumers and 300 messages each gets about 100, every message is acked exactly
    once overall, and the cursor is correct.
    """

    def test_3_consumers_300_messages_round_robin_and_cursor_correct(self, temp_dir):
        storage_server = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="Storage")
        storage_port = storage_server.start()

        broker_server = BrokerServer(
            host="127.0.0.1",
            port=0,
            storage_host="127.0.0.1",
            storage_port=storage_port,
            broker_id="broker-1",
            name="Broker-1"
        )
        broker_port = broker_server.start()

        topic = "heavy-workload"
        sub_name = "analytics-workers"
        total_messages = 300

        try:
            # 1. Publish 300 messages
            with Producer(broker_host="127.0.0.1", broker_port=broker_port) as producer:
                for i in range(total_messages):
                    producer.send(topic, f"payload-{i}")

            # 2. Connect 3 consumers in SHARED mode
            c1 = Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name, mode=SubscriptionModes.SHARED, consumer_id="worker-A")
            c2 = Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name, mode=SubscriptionModes.SHARED, consumer_id="worker-B")
            c3 = Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name, mode=SubscriptionModes.SHARED, consumer_id="worker-C")

            c1.connect()
            c2.connect()
            c3.connect()

            received_counts = {"worker-A": 0, "worker-B": 0, "worker-C": 0}
            all_received_offsets = set()
            lock = threading.Lock()

            def worker_loop(consumer: Consumer, name: str):
                while len(all_received_offsets) < total_messages:
                    msg = consumer.receive(timeout=1.0)
                    if msg is None:
                        continue
                    with lock:
                        all_received_offsets.add(msg.message_id.offset)
                        received_counts[name] += 1
                    consumer.ack(msg)

            t1 = threading.Thread(target=worker_loop, args=(c1, "worker-A"), daemon=True)
            t2 = threading.Thread(target=worker_loop, args=(c2, "worker-B"), daemon=True)
            t3 = threading.Thread(target=worker_loop, args=(c3, "worker-C"), daemon=True)

            t1.start()
            t2.start()
            t3.start()

            t1.join(timeout=10.0)
            t2.join(timeout=10.0)
            t3.join(timeout=10.0)

            # 3. Assertions
            # All 300 messages received
            assert len(all_received_offsets) == total_messages
            assert all_received_offsets == set(range(total_messages))

            # Each consumer got about 100 (e.g. between 70 and 130 in round robin)
            print(f"Distribution: {received_counts}")
            for name, count in received_counts.items():
                assert 70 <= count <= 130, f"{name} got {count} messages, expected ~100"

            # Sum of counts equals 300
            assert sum(received_counts.values()) == total_messages

            # 4. Verify cursor is correct (compacted to 299, empty individual_acks)
            key = (topic, sub_name)
            dispatcher = broker_server._dispatchers[key]
            # Ensure final acks persisted
            dispatcher._persist_cursor_now()

            assert dispatcher.cursor.mark_delete_position == total_messages - 1
            assert len(dispatcher.cursor.individual_acks) == 0

            c1.close()
            c2.close()
            c3.close()
        finally:
            broker_server.stop()
            storage_server.stop()
