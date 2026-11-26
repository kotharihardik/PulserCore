"""Phase 9 Validation Tests: Broker Failover, Topic Lookup, Client Reconnect & Integration Checkpoint C."""

import shutil
import tempfile
import time
import pytest

from broker.broker_server import BrokerServer
from client.consumer import Consumer
from client.producer import Producer
from common.protocol import SubscriptionModes
from coordinator.coordinator_client import CoordinatorClient
from coordinator.coordinator_server import CoordinatorServer
from storage.storage_client import StorageClient
from storage.storage_server import StorageServer


@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp(prefix="pulser_test_phase9_")
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


class TestBrokerOwnershipAndRedirect:
    """Test Coordinator lookup, NOT_OWNER rejection, and client redirect/retry."""

    def test_producer_redirects_from_non_owner_broker_via_coordinator(self, temp_dir):
        storage = StorageServer("127.0.0.1", 0, f"{temp_dir}/storage", name="Storage")
        s_port = storage.start()

        coord = CoordinatorServer("127.0.0.1", 0, f"{temp_dir}/coord", name="Coord")
        c_port = coord.start()

        broker1 = BrokerServer(
            "127.0.0.1", 0, "127.0.0.1", s_port,
            broker_id="broker-1", coordinator_host="127.0.0.1", coordinator_port=c_port,
            lease_ttl_ms=2000, name="Broker-1"
        )
        b1_port = broker1.start()

        broker2 = BrokerServer(
            "127.0.0.1", 0, "127.0.0.1", s_port,
            broker_id="broker-2", coordinator_host="127.0.0.1", coordinator_port=c_port,
            lease_ttl_ms=2000, name="Broker-2"
        )
        b2_port = broker2.start()

        topic = "orders"
        try:
            # Broker 1 claims ownership of 'orders'
            tok1 = broker1.acquire_topic(topic)
            assert tok1 == 1

            # Producer connects initially to WRONG broker (Broker 2), with coordinator enabled
            with Producer(
                broker_host="127.0.0.1",
                broker_port=b2_port,
                coordinator_host="127.0.0.1",
                coordinator_port=c_port
            ) as producer:
                # Broker 2 rejects with NOT_OWNER -> Producer looks up Coordinator -> Redirects to Broker 1!
                msg_id = producer.send(topic, {"order": "redirect-test"})
                assert msg_id.topic == topic
                assert msg_id.offset == 0
                assert producer.broker_port == b1_port
                assert producer.reconnect_count >= 1
        finally:
            broker1.stop()
            broker2.stop()
            coord.stop()
            storage.stop()

    def test_zombie_broker_is_fenced_after_standby_takeover(self, temp_dir):
        storage = StorageServer("127.0.0.1", 0, f"{temp_dir}/storage", name="Storage")
        s_port = storage.start()

        coord = CoordinatorServer("127.0.0.1", 0, f"{temp_dir}/coord", name="Coord")
        c_port = coord.start()

        topic = "payments"

        # Broker 1 owns topic with short lease (400 ms)
        broker1 = BrokerServer(
            "127.0.0.1", 0, "127.0.0.1", s_port,
            broker_id="broker-1", coordinator_host="127.0.0.1", coordinator_port=c_port,
            lease_ttl_ms=400, renew_interval_sec=5.0,  # Long renew interval simulates paused renewal!
            name="ZombieBroker"
        )
        b1_port = broker1.start()

        broker2 = BrokerServer(
            "127.0.0.1", 0, "127.0.0.1", s_port,
            broker_id="broker-2", coordinator_host="127.0.0.1", coordinator_port=c_port,
            lease_ttl_ms=2000, name="StandbyBroker"
        )
        b2_port = broker2.start()

        try:
            tok1 = broker1.acquire_topic(topic)
            assert tok1 == 1

            # Broker 1 writes initial message
            with Producer("127.0.0.1", b1_port) as p1:
                m0 = p1.send(topic, "pay-0")
                assert m0.offset == 0

            # Simulate Broker 1 pausing past 400ms lease TTL
            time.sleep(0.5)

            # Standby Broker 2 acquires expired lease with token = 2 and writes!
            tok2 = broker2.acquire_topic(topic)
            assert tok2 == 2
            with Producer("127.0.0.1", b2_port) as p2:
                m1 = p2.send(topic, "pay-1")
                assert m1.offset == 1

            # Now Zombie Broker 1 wakes up and attempts to publish directly using stale token 1!
            # Direct client without auto-reconnect should get FENCED and Broker 1 must self-demote!
            with Producer("127.0.0.1", b1_port) as stale_p:
                with pytest.raises(Exception) as exc:
                    stale_p.send(topic, "zombie-pay")
                assert "FENCED" in str(exc.value)

            # Broker 1 relinquished stale ownership locally
            assert broker1.get_owned_token(topic) is None
        finally:
            broker1.stop()
            broker2.stop()
            coord.stop()
            storage.stop()


class TestPhase9IntegrationCheckpointC:
    """Official Phase 9 Done When & Integration Checkpoint C:
    Kill the owning broker mid-stream; publishing and consuming resume on another broker
    and no acked message is lost.
    """

    def test_mid_stream_broker_failover_resumes_without_data_loss(self, temp_dir):
        storage = StorageServer("127.0.0.1", 0, f"{temp_dir}/storage", name="Storage")
        s_port = storage.start()

        coord = CoordinatorServer("127.0.0.1", 0, f"{temp_dir}/coord", name="Coord")
        c_port = coord.start()

        topic = "critical-orders"
        sub_name = "billing-sub"

        # Broker 1 (initial active owner) & Broker 2 (standby watching topic)
        broker1 = BrokerServer(
            "127.0.0.1", 0, "127.0.0.1", s_port,
            broker_id="broker-1", coordinator_host="127.0.0.1", coordinator_port=c_port,
            lease_ttl_ms=500, renew_interval_sec=0.15, watch_topics=[topic],
            name="Broker-1"
        )
        broker1.start()

        broker2 = BrokerServer(
            "127.0.0.1", 0, "127.0.0.1", s_port,
            broker_id="broker-2", coordinator_host="127.0.0.1", coordinator_port=c_port,
            lease_ttl_ms=2000, renew_interval_sec=0.15, watch_topics=[topic],
            name="Broker-2"
        )
        b2_port = broker2.start()

        try:
            # Ensure Broker-1 initially owns the topic
            tok1 = broker1.acquire_topic(topic)
            assert tok1 == 1

            # Connect Producer and Consumer via Coordinator discovery
            producer = Producer(
                coordinator_host="127.0.0.1",
                coordinator_port=c_port,
                max_retries=25,
                base_backoff_sec=0.1
            )
            producer.connect(topic)

            consumer = Consumer(
                topic=topic,
                subscription=sub_name,
                mode=SubscriptionModes.EXCLUSIVE,
                consumer_id="billing-worker-1",
                coordinator_host="127.0.0.1",
                coordinator_port=c_port,
                max_retries=25,
                base_backoff_sec=0.1
            )
            consumer.connect()

            # 1. Phase A: Publish and consume messages 0..9 on Broker-1
            acked_offsets = set()
            for i in range(10):
                msg_id = producer.send(topic, {"order_seq": i})
                assert msg_id.offset == i

            while len(acked_offsets) < 10:
                msg = consumer.receive(timeout=2.0)
                assert msg is not None
                acked_offsets.add(msg.message_id.offset)
                consumer.ack(msg)

            assert acked_offsets == set(range(10))

            # 2. CRASH Broker-1 abruptly mid-stream (without releasing its lease!)
            broker1.crash()

            # 3. Wait slightly for lease expiry + standby Broker-2 automatic takeover
            deadline = time.time() + 3.0
            while time.time() < deadline:
                with CoordinatorClient("127.0.0.1", c_port) as cc:
                    owner = cc.get_owner(topic)
                    if owner and owner["broker_id"] == "broker-2":
                        break
                time.sleep(0.05)

            assert broker2.get_owned_token(topic) is not None
            assert broker2.get_owned_token(topic) > tok1

            # 4. Phase B: Publish messages 10..19 — Producer automatically fails over to Broker-2!
            for i in range(10, 20):
                msg_id = producer.send(topic, {"order_seq": i})
                assert msg_id.offset == i

            # 5. Consumer automatically reconnects to Broker-2 and receives 10..19!
            deadline = time.time() + 5.0
            while len(acked_offsets) < 20 and time.time() < deadline:
                msg = consumer.receive(timeout=1.0)
                if msg is None:
                    continue
                acked_offsets.add(msg.message_id.offset)
                consumer.ack(msg)

            # Verify every single message 0..19 was delivered & acked (zero lost messages)
            assert acked_offsets == set(range(20))
            assert producer.broker_port == b2_port
            assert producer.reconnect_count >= 1
            assert consumer.broker_port == b2_port
            assert consumer.reconnect_count >= 1

            # Verify on storage disk that all 20 messages exist intact
            with StorageClient("127.0.0.1", s_port) as sc:
                assert sc.last_offset(topic) == 19

            producer.close()
            consumer.close()
        finally:
            broker1.stop(release_leases=False)
            broker2.stop()
            coord.stop()
            storage.stop()

