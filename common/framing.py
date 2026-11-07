"""Binary length-prefixed framing over TCP sockets.

Protocol:
[4 bytes: Big-Endian Unsigned Integer N] + [N bytes: UTF-8 JSON String]
"""

import json
import socket
import struct
from typing import Any, Dict, Optional

HEADER_FORMAT = "!I"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 4 bytes
MAX_FRAME_SIZE = 16 * 1024 * 1024  # 16 MB limit


class FramingError(Exception):
    """Raised when framing constraints are violated."""
    pass


def encode_frame(data: Dict[str, Any]) -> bytes:
    """Serializes a dictionary to JSON and prefixes it with a 4-byte length header."""
    payload_str = json.dumps(data)
    payload_bytes = payload_str.encode("utf-8")
    payload_len = len(payload_bytes)

    if payload_len > MAX_FRAME_SIZE:
        raise FramingError(f"Payload size {payload_len} exceeds limit {MAX_FRAME_SIZE}")

    header = struct.pack(HEADER_FORMAT, payload_len)
    return header + payload_bytes


def recv_exact(sock: socket.socket, num_bytes: int) -> Optional[bytes]:
    """Reads exactly `num_bytes` from the socket.
    
    Returns:
        bytes: The exact bytes requested.
        None: If the remote end closed the connection cleanly before any bytes were read.
    
    Raises:
        ConnectionError: If the socket closes mid-stream after partial read.
        socket.timeout: If socket timeout triggers.
    """
    buf = bytearray()
    while len(buf) < num_bytes:
        try:
            chunk = sock.recv(num_bytes - len(buf))
        except (BlockingIOError, InterruptedError):
            continue
        except (ConnectionResetError, BrokenPipeError):
            if len(buf) == 0:
                return None
            raise ConnectionError(f"Connection reset after reading {len(buf)} of {num_bytes} bytes")

        if not chunk:
            if len(buf) == 0:
                return None  # Clean connection close
            raise ConnectionError(f"Remote closed stream prematurely. Got {len(buf)} of {num_bytes} bytes")

        buf.extend(chunk)

    return bytes(buf)


def read_frame(sock: socket.socket) -> Optional[Dict[str, Any]]:
    """Reads a single framed message from a socket.
    
    Returns:
        dict: The deserialized JSON object.
        None: If the socket was closed cleanly before any new frame arrived.
    
    Raises:
        FramingError: If the frame exceeds max length or is malformed JSON.
        ConnectionError: If stream drops midway through a frame.
    """
    header = recv_exact(sock, HEADER_SIZE)
    if header is None:
        return None

    payload_len = struct.unpack(HEADER_FORMAT, header)[0]
    if payload_len == 0:
        return {}

    if payload_len > MAX_FRAME_SIZE:
        raise FramingError(f"Incoming frame size {payload_len} exceeds maximum allowed {MAX_FRAME_SIZE}")

    payload_bytes = recv_exact(sock, payload_len)
    if payload_bytes is None:
        raise ConnectionError(f"Socket disconnected while reading {payload_len} payload bytes")

    try:
        payload_str = payload_bytes.decode("utf-8")
        return json.loads(payload_str)
    except UnicodeDecodeError as e:
        raise FramingError(f"Invalid UTF-8 payload: {e}") from e
    except json.JSONDecodeError as e:
        raise FramingError(f"Invalid JSON payload: {e}") from e


def write_frame(sock: socket.socket, data: Dict[str, Any]) -> None:
    """Encodes a dictionary and transmits the complete frame over the socket."""
    frame_bytes = encode_frame(data)
    sock.sendall(frame_bytes)

