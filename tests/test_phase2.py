"""Phase 2 Validation Tests: Ownership Coordinator, Leases & Fencing Tokens."""

import os
import shutil
import tempfile
import time
import pytest

from coordinator.lease_manager import LeaseError, LeaseManager
from coordinator.coordinator_client import CoordinatorClient
from coordinator.coordinator_server import CoordinatorServer


@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp(prefix="pulser_test_coordinator_")
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


class TestLeaseManagerBasics:
    """Test core lease lifecycle, exclusivity, and renewals."""

    def test_acquire_and_renew(self, temp_dir):
        manager = LeaseManager(temp_dir)
        token, exp1 = manager.acquire("topic-1", "broker-A", lease_ttl_ms=1000)
        assert token == 1
        assert exp1 > time.time()

        # Renew
        exp2 = manager.renew("topic-1", "broker-A", token=1, lease_ttl_ms=1000)
        assert exp2 >= exp1

        # Query owner
        owner = manager.get_owner("topic-1")
        assert owner is not None
        assert owner["broker_id"] == "broker-A"
        assert owner["fencing_token"] == 1

    def test_exclusivity_blocks_competing_broker(self, temp_dir):
        manager = LeaseManager(temp_dir)
        manager.acquire("topic-1", "broker-A", lease_ttl_ms=2000)

        # Broker B tries to acquire active lease
        with pytest.raises(LeaseError) as exc_info:
            manager.acquire("topic-1", "broker-B", lease_ttl_ms=2000)
        assert "currently owned by 'broker-A'" in str(exc_info.value)

    def test_release_allows_immediate_takeover(self, temp_dir):
        manager = LeaseManager(temp_dir)
        t1, _ = manager.acquire("topic-1", "broker-A", lease_ttl_ms=5000)
        assert t1 == 1

        released = manager.release("topic-1", "broker-A", token=1)
        assert released is True

        # Broker B can immediately acquire without waiting for TTL
        t2, _ = manager.acquire("topic-1", "broker-B", lease_ttl_ms=5000)
        assert t2 == 2


class TestCoordinatorPersistence:
    """Verify fencing counter survives coordinator restarts and never rewinds."""

    def test_counter_durability_across_restarts(self, temp_dir):
        # Coordinator 1 issues tokens 1, 2, 3
        coord1 = LeaseManager(temp_dir)
        t1, _ = coord1.acquire("topic-1", "broker-A")
        t2, _ = coord1.acquire("topic-2", "broker-B")
        t3, _ = coord1.acquire("topic-3", "broker-C")
        assert (t1, t2, t3) == (1, 2, 3)

        # Crash / restart coordinator
        coord2 = LeaseManager(temp_dir)
        # Next token MUST be 4
        t4, _ = coord2.acquire("topic-4", "broker-D")
        assert t4 == 4


class TestPhase2DoneWhenCriterion:
    """Official Phase 2 Done When Criterion:
    Broker A holds the lease, stops renewing, broker B acquires with token N+1,
    and A's renewal is rejected.
    """

    def test_broker_a_expires_broker_b_acquires_n_plus_1_and_a_renewal_rejected(self, temp_dir):
        server = CoordinatorServer(host="127.0.0.1", port=0, data_dir=temp_dir, name="TestCoord")
        port = server.start()

        try:
            with CoordinatorClient(host="127.0.0.1", port=port) as client_a, \
                 CoordinatorClient(host="127.0.0.1", port=port) as client_b:

                topic = "orders"

                # 1. Broker A acquires the lease with short TTL (400 ms)
                token_a, exp_a = client_a.acquire_lease(topic, broker_id="broker-A", lease_ttl_ms=400)
                assert token_a == 1

                # Verify Broker A is recorded as owner
                owner_info = client_a.get_owner(topic)
                assert owner_info["broker_id"] == "broker-A"
                assert owner_info["fencing_token"] == token_a

                # 2. Broker A stops renewing. Sleep until lease expires (> 400 ms)
                time.sleep(0.5)

                # 3. Broker B acquires the expired lease
                token_b, exp_b = client_b.acquire_lease(topic, broker_id="broker-B", lease_ttl_ms=2000)
                # Token must be strictly higher (N + 1)
                assert token_b == token_a + 1
                assert token_b == 2

                # Verify Broker B is now the registered owner
                owner_now = client_b.get_owner(topic)
                assert owner_now["broker_id"] == "broker-B"
                assert owner_now["fencing_token"] == 2

                # 4. Broker A wakes up from pause and attempts renewal with stale token
                with pytest.raises(LeaseError) as exc_info:
                    client_a.renew_lease(topic, broker_id="broker-A", token=token_a, lease_ttl_ms=1000)

                assert "Renewal rejected" in str(exc_info.value)
        finally:
            server.stop()

