"""CLI entrypoint to run individual PulserCore nodes as standalone OS processes."""

import argparse
import signal
import sys
import threading
import time

from broker.broker_server import BrokerServer
from coordinator.coordinator_server import CoordinatorServer
from storage.storage_server import StorageServer


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a standalone PulserCore cluster node.")
    subparsers = parser.add_subparsers(dest="role", required=True)

    # Storage Node
    p_storage = subparsers.add_parser("storage", help="Run Storage Service node")
    p_storage.add_argument("--host", default="127.0.0.1")
    p_storage.add_argument("--port", type=int, default=5000)
    p_storage.add_argument("--data-dir", required=True)
    p_storage.add_argument("--name", default="StorageNode")

    # Coordinator Node
    p_coord = subparsers.add_parser("coordinator", help="Run Ownership Coordinator node")
    p_coord.add_argument("--host", default="127.0.0.1")
    p_coord.add_argument("--port", type=int, default=6000)
    p_coord.add_argument("--data-dir", required=True)
    p_coord.add_argument("--name", default="CoordinatorNode")

    # Broker Node
    p_broker = subparsers.add_parser("broker", help="Run Stateless Broker node")
    p_broker.add_argument("--host", default="127.0.0.1")
    p_broker.add_argument("--port", type=int, default=7001)
    p_broker.add_argument("--broker-id", required=True)
    p_broker.add_argument("--storage-host", default="127.0.0.1")
    p_broker.add_argument("--storage-port", type=int, required=True)
    p_broker.add_argument("--coord-host", default="127.0.0.1")
    p_broker.add_argument("--coord-port", type=int, required=True)
    p_broker.add_argument("--lease-ttl-ms", type=int, default=1000)
    p_broker.add_argument("--renew-interval-sec", type=float, default=None)
    p_broker.add_argument("--ack-timeout-sec", type=float, default=1.0)
    p_broker.add_argument("--watch-topics", nargs="*", default=[])

    args = parser.parse_args()
    stop_event = threading.Event()

    if args.role == "storage":
        node = StorageServer(args.host, args.port, args.data_dir, name=args.name)
    elif args.role == "coordinator":
        node = CoordinatorServer(args.host, args.port, args.data_dir, name=args.name)
    elif args.role == "broker":
        node = BrokerServer(
            host=args.host,
            port=args.port,
            storage_host=args.storage_host,
            storage_port=args.storage_port,
            broker_id=args.broker_id,
            ack_timeout_sec=args.ack_timeout_sec,
            coordinator_host=args.coord_host,
            coordinator_port=args.coord_port,
            lease_ttl_ms=args.lease_ttl_ms,
            renew_interval_sec=args.renew_interval_sec,
            watch_topics=args.watch_topics,
            name=args.broker_id,
        )
    else:
        sys.exit(1)

    def _handle_sigterm(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    bound_port = node.start()
    print(f"NODE_READY role={args.role} port={bound_port}", flush=True)

    try:
        while not stop_event.is_set():
            time.sleep(0.1)
    finally:
        node.stop()


if __name__ == "__main__":
    main()

