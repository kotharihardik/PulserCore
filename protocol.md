# PulserCore Wire Protocol Specification

Version: 1.0.0

## 1. Physical Framing (TCP Transport)

TCP is a stream-oriented transport protocol without inherent message boundaries. To guarantee reliable message framing over TCP without delimiter collision (like `\n`), PulserCore uses a standard **4-byte length-prefixed binary framing**:

```
+-----------------------------------+---------------------------------------+
|  Length (4 bytes, Big-Endian uint)|      JSON Payload (UTF-8 Encoded)     |
|             (N bytes)             |                (N bytes)              |
+-----------------------------------+---------------------------------------+
```

### Framing Rules:
1. Every frame begins with exactly 4 bytes representing an unsigned 32-bit integer in network byte order (Big-Endian, `!I` in Python `struct`).
2. The integer value `N` denotes the byte length of the UTF-8 payload immediately following the header.
3. The receiver must read exactly 4 bytes to determine `N`, then read exactly `N` bytes before attempting to deserialize the JSON payload.
4. Maximum payload size default is 16 MB (`16 * 1024 * 1024` bytes) to prevent out-of-memory denial of service.

---

## 2. Message Identification (`MessageId`)

Because message storage is decoupled and managed by the storage service per topic, a message's immutable identity is defined by its topic and its sequential append-only offset:

```json
{
  "topic": "orders",
  "offset": 1042
}
```

* String representation: `"<topic>:<offset>"` (e.g., `"orders:1042"`).

---

## 3. Standard Request/Response Format

Every JSON payload MUST be a JSON object with at least:
* `type`: The command or response type (uppercase string).
* `req_id`: Optional or recommended correlation ID for asynchronous matching (UUID string or integer).

### Standard Error Response
Whenever a request fails, the service returns an `ERROR` frame:
```json
{
  "type": "ERROR",
  "req_id": "req-123",
  "error_code": "FENCED",
  "message": "Write rejected: broker token 2 is lower than highest known token 3"
}
```

Standard Error Codes:
* `NOT_OWNER`: Broker contacted is not the current topic owner.
* `FENCED`: Write rejected because broker's fencing token is stale.
* `CONSUMER_BUSY`: Exclusive subscription already has an active connected consumer.
* `TOPIC_NOT_FOUND`: Specified topic does not exist.
* `INVALID_OFFSET`: Requested offset is out of bounds or pruned.
* `LEASE_NOT_FOUND`: Attempted to renew or release a non-existent or expired lease.
* `BAD_REQUEST`: Malformed command or missing required fields.
* `INTERNAL_ERROR`: Unexpected internal exception on the node.

---

## 4. Commands Specification

### 4.1 Storage Service API (Broker <---> Storage)

The storage service persists append-only logs, enforces fencing checks, and maintains subscription cursor checkpoints.

#### `APPEND` (Write message)
Sent by broker to append a message to a topic.
```json
{
  "type": "APPEND",
  "req_id": "1",
  "topic": "orders",
  "payload": "{\"order_id\": 99, \"amount\": 450}",
  "fencing_token": 5
}
```
**Response: `APPEND_OK`**
```json
{
  "type": "APPEND_OK",
  "req_id": "1",
  "topic": "orders",
  "offset": 1043
}
```
*If `fencing_token < storage.highest_token[topic]`, returns `ERROR` with code `FENCED`.*

#### `READ` (Read range)
Read sequential messages starting from an offset.
```json
{
  "type": "READ",
  "req_id": "2",
  "topic": "orders",
  "from_offset": 1000,
  "max_count": 50
}
```
**Response: `READ_OK`**
```json
{
  "type": "READ_OK",
  "req_id": "2",
  "topic": "orders",
  "messages": [
    {"offset": 1000, "payload": "...", "timestamp": 1730000000.12},
    {"offset": 1001, "payload": "...", "timestamp": 1730000000.15}
  ],
  "next_offset": 1002
}
```

#### `LAST_OFFSET` (Get newest offset)
Query the latest written offset for a topic.
```json
{
  "type": "LAST_OFFSET",
  "req_id": "3",
  "topic": "orders"
}
```
**Response: `LAST_OFFSET_OK`**
```json
{
  "type": "LAST_OFFSET_OK",
  "req_id": "3",
  "topic": "orders",
  "last_offset": 1043
}
```
*(Returns -1 if topic has no messages).*

#### `SAVE_CURSOR` (Persist subscription progress)
Broker checkpoints subscription state.
```json
{
  "type": "SAVE_CURSOR",
  "req_id": "4",
  "topic": "orders",
  "subscription": "billing-service",
  "mark_delete_offset": 1020,
  "individual_acks": [1022, 1025],
  "fencing_token": 5
}
```
**Response: `SAVE_CURSOR_OK`**
```json
{
  "type": "SAVE_CURSOR_OK",
  "req_id": "4",
  "topic": "orders",
  "subscription": "billing-service"
}
```

#### `LOAD_CURSOR` (Retrieve subscription progress)
Broker retrieves subscription state on startup or topic takeover.
```json
{
  "type": "LOAD_CURSOR",
  "req_id": "5",
  "topic": "orders",
  "subscription": "billing-service"
}
```
**Response: `LOAD_CURSOR_OK`**
```json
{
  "type": "LOAD_CURSOR_OK",
  "req_id": "5",
  "topic": "orders",
  "subscription": "billing-service",
  "mark_delete_offset": 1020,
  "individual_acks": [1022, 1025]
}
```

---

### 4.2 Coordinator API (Broker/Client <---> Coordinator)

The Coordinator manages time-bound topic leases with monotonically increasing fencing tokens.

#### `ACQUIRE_LEASE`
Broker requests ownership of a topic.
```json
{
  "type": "ACQUIRE_LEASE",
  "req_id": "6",
  "topic": "orders",
  "broker_id": "broker-1",
  "lease_ttl_ms": 5000
}
```
**Response: `ACQUIRE_LEASE_OK`**
```json
{
  "type": "ACQUIRE_LEASE_OK",
  "req_id": "6",
  "topic": "orders",
  "broker_id": "broker-1",
  "fencing_token": 5,
  "expires_at": 1730000005.5
}
```

#### `RENEW_LEASE`
Owning broker periodically heartbeats to extend its lease.
```json
{
  "type": "RENEW_LEASE",
  "req_id": "7",
  "topic": "orders",
  "broker_id": "broker-1",
  "fencing_token": 5,
  "lease_ttl_ms": 5000
}
```
**Response: `RENEW_LEASE_OK`**
```json
{
  "type": "RENEW_LEASE_OK",
  "req_id": "7",
  "topic": "orders",
  "broker_id": "broker-1",
  "fencing_token": 5,
  "expires_at": 1730000010.5
}
```

#### `RELEASE_LEASE`
Broker gracefully gives up ownership (e.g. during clean shutdown).
```json
{
  "type": "RELEASE_LEASE",
  "req_id": "8",
  "topic": "orders",
  "broker_id": "broker-1",
  "fencing_token": 5
}
```
**Response: `RELEASE_LEASE_OK`**
```json
{
  "type": "RELEASE_LEASE_OK",
  "req_id": "8",
  "topic": "orders"
}
```

#### `GET_OWNER`
Clients and standby brokers discover which broker currently owns a topic.
```json
{
  "type": "GET_OWNER",
  "req_id": "9",
  "topic": "orders"
}
```
**Response: `GET_OWNER_OK`**
```json
{
  "type": "GET_OWNER_OK",
  "req_id": "9",
  "topic": "orders",
  "broker_id": "broker-1",
  "broker_address": "127.0.0.1:5001",
  "fencing_token": 5
}
```
*(If no broker currently holds the lease, `broker_id` is null).*

---

### 4.3 Client-Broker API (Producer / Consumer <---> Broker)

#### `PUBLISH` (Producer -> Broker)
Producer sends a new payload to the topic owner.
```json
{
  "type": "PUBLISH",
  "req_id": "10",
  "topic": "orders",
  "payload": "{\"item\": \"book\", \"qty\": 2}"
}
```
**Response: `PUBLISH_OK`**
```json
{
  "type": "PUBLISH_OK",
  "req_id": "10",
  "topic": "orders",
  "message_id": {
    "topic": "orders",
    "offset": 1044
  }
}
```
*If broker is not the owner, returns `ERROR` with `error_code: NOT_OWNER` and redirects with owner address.*

#### `SUBSCRIBE` (Consumer -> Broker)
Consumer registers a subscription under a given mode.
```json
{
  "type": "SUBSCRIBE",
  "req_id": "11",
  "topic": "orders",
  "subscription": "sub-1",
  "mode": "EXCLUSIVE",
  "consumer_id": "client-worker-1"
}
```
*Modes:* `"EXCLUSIVE"` or `"SHARED"`
**Response: `SUBSCRIBE_OK`**
```json
{
  "type": "SUBSCRIBE_OK",
  "req_id": "11",
  "topic": "orders",
  "subscription": "sub-1",
  "mode": "EXCLUSIVE"
}
```
*If mode is `EXCLUSIVE` and another consumer is already connected, returns `ERROR` with `CONSUMER_BUSY`.*

#### `DELIVER` (Broker -> Consumer - Unsolicited push)
Broker pushes messages to connected consumers.
```json
{
  "type": "DELIVER",
  "topic": "orders",
  "subscription": "sub-1",
  "message_id": {
    "topic": "orders",
    "offset": 1044
  },
  "payload": "{\"item\": \"book\", \"qty\": 2}",
  "redelivery_count": 0
}
```

#### `ACK` (Consumer -> Broker)
Consumer acknowledges successful processing of a message.
```json
{
  "type": "ACK",
  "req_id": "12",
  "topic": "orders",
  "subscription": "sub-1",
  "message_id": {
    "topic": "orders",
    "offset": 1044
  }
}
```
**Response: `ACK_OK`**
```json
{
  "type": "ACK_OK",
  "req_id": "12",
  "topic": "orders",
  "subscription": "sub-1",
  "offset": 1044
}
```

