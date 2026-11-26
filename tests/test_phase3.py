"""Phase 3 Validation Tests: Broker Publish Path, Producer Client & Integration Checkpoint A."""

import os
import shutil
import tempfile
import pytest

from broker.broker_server import BrokerServer
from client.producer import Producer, PublishError
from common.protocol import ErrorCodes, MessageId
from storage.storage_client import StorageClient
from storage.storage_server import StorageServer


@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp(prefix="pulser_test_phase3_")
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


class TestProducerAndBroker:
    """Test broker publish path and producer client library."""

    def test_producer_sends_messages_and_receives_increasing_message_ids(self, temp_dir):
        """Phase 3 Done When Criterion:
        A producer sends messages and receives increasing MessageIds.
        """
        # 1. Start Storage
        storage_server = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="Storage")
        storage_port = storage_server.start()

        # 2. Start Broker
        broker_server = BrokerServer(
            host="127.0.0.1",
            port=0,
            storage_host="127.0.0.1",
            storage_port=storage_port,
            broker_id="broker-1",
            fencing_token=1,
            name="Broker-1"
        )
        broker_port = broker_server.start()

        try:
            # 3. Connect Producer to Broker
            with Producer(broker_host="127.0.0.1", broker_port=broker_port) as producer:
                topic = "orders"

                msg_id_0 = producer.send(topic, {"order_id": 101, "item": "book"})
                assert isinstance(msg_id_0, MessageId)
                assert msg_id_0.topic == topic
                assert msg_id_0.offset == 0

                msg_id_1 = producer.send(topic, {"order_id": 102, "item": "laptop"})
                assert msg_id_1.offset == 1

                msg_id_2 = producer.send(topic, "simple-string-payload")
                assert msg_id_2.offset == 2

                # Verify strictly increasing offsets
                assert msg_id_0.offset < msg_id_1.offset < msg_id_2.offset
        finally:
            broker_server.stop()
            storage_server.stop()

    def test_broker_propagates_fenced_error_to_producer(self, temp_dir):
        """Verify that when storage rejects a broker's token, broker notifies producer."""
        storage_server = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="Storage")
        storage_port = storage_server.start()

        # Advance storage highest token to 10
        with StorageClient("127.0.0.1", storage_port) as sc:
            sc.append("payments", "prime", fencing_token=10)

        # Broker has stale fencing token = 5
        stale_broker = BrokerServer(
            host="127.0.0.1",
            port=0,
            storage_host="127.0.0.1",
            storage_port=storage_port,
            fencing_token=5,
            name="StaleBroker"
        )
        broker_port = stale_broker.start()

        try:
            with Producer(broker_host="127.0.0.1", broker_port=broker_port) as producer:
                with pytest.raises(PublishError) as exc_info:
                    producer.send("payments", "should-fail")

                assert exc_info.value.error_code == ErrorCodes.FENCED
                assert "fenced out" in exc_info.value.message.lower()
        finally:
            stale_broker.stop()
            storage_server.stop()


class TestIntegrationCheckpointA:
    """Official Integration Checkpoint A:
    Producer -> Broker -> Real Storage, with messages persisted on disk.
    """

    def test_checkpoint_a_end_to_end_durability(self, temp_dir):
        storage_server = StorageServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="CheckpointStorage")
        storage_port = storage_server.start()

        broker_server = BrokerServer(
            host="127.0.0.1",
            port=0,
            storage_host="127.0.0.1",
            storage_port=storage_port,
            broker_id="broker-prod",
            fencing_token=1,
            name="CheckpointBroker"
        )
        broker_port = broker_server.start()

        topic = "sensor-telemetry"
        num_messages = 50

        try:
            # 1. Producer publishes 50 messages to Broker
            with Producer(broker_host="127.0.0.1", broker_port=broker_port) as producer:
                for i in range(num_messages):
                    msg_id = producer.send(topic, {"sensor_id": "temp-sensor", "reading": i * 1.5})
                    assert msg_id.topic == topic
                    assert msg_id.offset == i

            # 2. Verify directly from Storage node that all 50 messages exist on disk
            with StorageClient("127.0.0.1", storage_port) as storage_client:
                assert storage_client.last_offset(topic) == num_messages - 1

                records, next_off = storage_client.read(topic, from_offset=0, max_count=100)
                assert len(records) == num_messages
                assert next_off == num_messages

                for i, rec in enumerate(records):
                    assert rec["offset"] == i
                    assert f'"reading": {i * 1.5}' in rec["payload"]
        finally:
            broker_server.stop()
            storage_server.stop()

