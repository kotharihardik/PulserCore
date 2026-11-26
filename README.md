# PulserCore

A cloud-native, distributed publish-subscribe messaging system inspired by **Apache Pulsar**, built in Python 3 from first principles. PulserCore separates **stateless broker routing** from **durable append-only log storage** and **lease-based topic ownership with monotonic fencing tokens**.

---

## Architecture

```
Producers / Consumers  <──(4-byte len + JSON over TCP)──>  Stateless Brokers (x3)
                                                               │            │
                                          APPEND / READ /      │            │ ACQUIRE / RENEW /
                                          SAVE_CURSOR          ▼            ▼ GET_OWNER
                                                      Storage Node      Coordinator Node
                                                   (fsync + Fencing)   (Leases + Monotonic Tokens)
```

### Core Modules
| Directory | Responsibility |
| :--- | :--- |
| [`common/`](common/) | 4-byte length-prefixed TCP framing (`recv_exact`), wire protocol constants, `MessageId`, `Cursor` (mark-delete + individual ack compaction), `BaseDispatcher`, thread-safe `TCPClient`/`BaseTCPServer`. |
| [`storage/`](storage/) | `LogManager` (append-only logs with `os.fsync()`, seek index, fencing check, durable cursors, time/size retention rules), `StorageServer`, `StorageClient`. |
| [`coordinator/`](coordinator/) | `LeaseManager` (time-bound leases, disk-persisted monotonic fencing token counter), `CoordinatorServer`, `CoordinatorClient`. |
| [`broker/`](broker/) | `BrokerServer` (lease heartbeats & standby takeover watchdog), `ExclusiveDispatcher` (strict FIFO + `CONSUMER_BUSY`), `SharedDispatcher` (round-robin + disconnect re-pooling), `UnackedTracker` (min-heap timeout watchdog). |
| [`client/`](client/) | `Producer` (sync durability + `GET_OWNER` discovery/backoff reconnect), `Consumer` (background frame demultiplexer + auto-reconnect). |
| [`scripts/`](scripts/) | Multi-process cluster runner (`run_node.py`), chaos fault-injection harness (`fault_harness.py`), multi-run evaluation & plotting (`evaluate.py`), interactive live demo (`demo.py`). |
| [`tests/`](tests/) | Comprehensive phase-by-phase test suite (`test_phase0.py` through `test_phase11.py`). |

---

## Quickstart

### Prerequisites
- Python 3.10+
- `pytest` (for tests) and `matplotlib` (for benchmark charts)

### 1. Run the Full Test Suite
```bash
python3 -m pytest tests/ -v
```

### 2. Run the Interactive Live Demo (5-Node Cluster)
Spawns 1 Storage node, 1 Coordinator node, and 3 Brokers on localhost, demonstrating Exclusive vs Shared subscriptions, live `SIGKILL` broker failover, and `SIGSTOP`/`SIGCONT` zombie fencing:
```bash
python3 -m scripts.demo
```

### 3. Run Automated Chaos Benchmarks & Plots
```bash
# Single command all-scenario run -> results/metrics.csv
python3 -m scripts.fault_harness --scenario all --output results/metrics.csv

# Multi-trial evaluation + summary CSV + matplotlib PNG charts
python3 -m scripts.evaluate --runs 3
```

---

## Key Distributed Systems Guarantees
- **Durability**: Every `APPEND`, cursor checkpoint, and fencing counter increment is flushed with `os.fsync()`.
- **Split-Brain Safety**: Storage enforces `fencing_token >= highest_seen_token`, rejecting stale zombie brokers with `FENCED`.
- **Delivery Semantics**: At-least-once delivery backed by a min-heap `UnackedTracker` (`O(1)` expiry checks) and explicit `MessageId` acknowledgments.
- **Ordering**:
  - **Exclusive**: Strict per-topic FIFO order (rewinds to `mark_delete_position` on crash/failover; duplicates possible).
  - **Shared**: Round-robin parallel distribution with no global ordering guarantee.
- **Retention**: Deletes old log prefixes only when past time/size limits **and** acknowledged by every active subscription.

See [`REPORT.md`](REPORT.md) for the full experimental evaluation and [`protocol.md`](protocol.md) for the wire protocol specification.
