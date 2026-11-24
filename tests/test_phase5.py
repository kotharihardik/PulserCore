"""Phase 5 Validation Tests: Exclusive Subscription, Strict FIFO Ordering & Consumer Reconnection."""

import shutil
import tempfile
import time
import pytest

from broker.broker_server import BrokerServer
from client.consumer import Consumer, ConsumerBusyError
from client.producer import Producer
from storage.storage_server import StorageServer


@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp(prefix="pulser_test_phase5_")
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


class TestExclusiveSubscription:
    """Test exclusivity, busy rejection, and consumer handoff."""

    def test_exclusive_mode_rejects_second_consumer_with_consumer_busy(self, temp_dir):
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
        sub_name = "exclusive-billing"

        try:
            # 1. Consumer 1 subscribes
            c1 = Consumer(
                broker_host="127.0.0.1",
                broker_port=broker_port,
                topic=topic,
                subscription=sub_name,
                consumer_id="consumer-1"
            )
            c1.connect()

            # 2. Consumer 2 attempts to subscribe to same exclusive sub -> Must raise ConsumerBusyError
            c2 = Consumer(
                broker_host="127.0.0.1",
                broker_port=broker_port,
                topic=topic,
                subscription=sub_name,
                consumer_id="consumer-2"
            )
            with pytest.raises(ConsumerBusyError):
                c2.connect()

            # 3. Consumer 1 disconnects
            c1.close()
            time.sleep(0.1)

            # 4. Consumer 2 can now successfully acquire the exclusive subscription
            c2.connect()
            c2.close()

        finally:
            broker_server.stop()
            storage_server.stop()

    def test_strict_fifo_ordering(self, temp_dir):
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

        topic = "strict-events"
        sub_name = "fifo-sub"

        try:
            # Publish 10 messages
            with Producer(broker_host="127.0.0.1", broker_port=broker_port) as producer:
                for i in range(10):
                    producer.send(topic, {"seq": i})

            # Consume 10 messages and verify exact FIFO order
            with Consumer(broker_host="127.0.0.1", broker_port=broker_port, topic=topic, subscription=sub_name) as consumer:
                for expected_seq in range(10):
                    msg = consumer.receive(timeout=2.0)
                    assert msg is not None
                    assert msg.message_id.offset == expected_seq
                    assert msg.value == {"seq": expected_seq}
                    consumer.ack(msg)

        finally:
            broker_server.stop()
            storage_server.stop()


class TestPhase5DoneWhenCriterion:
    """Official Phase 5 Done When Criterion:
    A consumer receives messages in order, restarts, and resumes from its last ack without skipping any.
    """

    def test_consumer_restarts_and_resumes_from_last_ack(self, temp_dir):
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

        topic = "resumable-jobs"
        sub_name = "worker-sub"

        try:
            # 1. Publish initial 10 messages (0..9)
            with Producer(broker_host="127.0.0.1", broker_port=broker_port) as producer:
                for i in range(10):
                    producer.send(topic, f"job-{i}")

            # 2. Consumer 1 connects, receives 0..4, acks each, then crashes / closes
            c1 = Consumer(
                broker_host="127.0.0.1",
                broker_port=broker_port,
                topic=topic,
                subscription=sub_name,
                consumer_id="worker-node-1"
            )
            c1.connect()

            for i in range(5):
                msg = c1.receive(timeout=2.0)
                assert msg is not None
                assert msg.message_id.offset == i
                assert msg.payload == f"job-{i}"
                c1.ack(msg)

            # Abrupt disconnect
            c1.close()
            time.sleep(0.1)

            # 3. Producer publishes 5 MORE messages (10..14)
            with Producer(broker_host="127.0.0.1", broker_port=broker_port) as producer:
                for i in range(10, 15):
                    producer.send(topic, f"job-{i}")

            # 4. Consumer 2 (restarted consumer) connects to same subscription
            c2 = Consumer(
                broker_host="127.0.0.1",
                broker_port=broker_port,
                topic=topic,
                subscription=sub_name,
                consumer_id="worker-node-2"
            )
            c2.connect()

            # Consumer 2 must resume from offset 5 through offset 14!
            # Nothing skipped, nothing duplicated!
            for expected_offset in range(5, 15):
                msg = c2.receive(timeout=2.0)
                assert msg is not None, f"Expected message with offset {expected_offset}"
                assert msg.message_id.offset == expected_offset
                assert msg.payload == f"job-{expected_offset}"
                c2.ack(msg)

            c2.close()

        finally:
            broker_server.stop()
            storage_server.stop()
