"""Phase 0 Validation Tests: Framing, Wire Protocol, and Stub Server."""

import socket
import threading
import time
import pytest

from common.dispatcher_base import BaseDispatcher
from common.framing import FramingError, encode_frame, read_frame, recv_exact, write_frame
from common.protocol import Commands, ErrorCodes, MessageId, make_error, make_ok
from common.tcp_service import BaseTCPServer, TCPClient


class TestMessageId:
    """Test MessageId immutability, serialization, and conversions."""

    def test_message_id_dict_conversion(self):
        msg_id = MessageId(topic="test-topic", offset=42)
        d = msg_id.to_dict()
        assert d == {"topic": "test-topic", "offset": 42}
        reconstructed = MessageId.from_dict(d)
        assert reconstructed == msg_id

    def test_message_id_string_conversion(self):
        msg_id = MessageId(topic="orders", offset=100)
        s = msg_id.to_string()
        assert s == "orders:100"
        reconstructed = MessageId.from_string(s)
        assert reconstructed == msg_id


class TestFraming:
    """Test TCP framing, length-prefixing, and fragmentation resilience."""

    def test_encode_and_read_frame(self):
        # Create a socket pair to test real network socket I/O
        server_sock, client_sock = socket.socketpair()
        try:
            sample_data = {
                "type": Commands.PUBLISH,
                "topic": "sensor-data",
                "payload": "temperature=25.4"
            }
            write_frame(client_sock, sample_data)
            received = read_frame(server_sock)
            assert received == sample_data
        finally:
            server_sock.close()
            client_sock.close()

    def test_fragmented_stream_handling(self):
        """Simulate worst-case TCP fragmentation: sending bytes 1 by 1."""
        server_sock, client_sock = socket.socketpair()
        try:
            sample_data = {"type": "ECHO", "data": "a" * 500}
            encoded_bytes = encode_frame(sample_data)

            # Send bytes one by one in a separate thread
            def slow_sender():
                for b in encoded_bytes:
                    client_sock.sendall(bytes([b]))
                    time.sleep(0.001)

            t = threading.Thread(target=slow_sender)
            t.start()

            # Receiver must wait and reconstruct the entire frame accurately
            received = read_frame(server_sock)
            t.join()
            assert received == sample_data
        finally:
            server_sock.close()
            client_sock.close()

    def test_oversized_frame_rejected(self):
        huge_data = {"huge": "x" * 100}
        # Craft frame header with length exceeding MAX_FRAME_SIZE
        fake_header = (17 * 1024 * 1024).to_bytes(4, byteorder="big")
        server_sock, client_sock = socket.socketpair()
        try:
            client_sock.sendall(fake_header + b"{}")
            with pytest.raises(FramingError):
                read_frame(server_sock)
        finally:
            server_sock.close()
            client_sock.close()


class StubEchoServer(BaseTCPServer):
    """Stub server for validating client-server communication in Phase 0."""

    def handle_request(self, request, client_sock):
        req_type = request.get("type")
        req_id = request.get("req_id")
        if req_type == "PING":
            return make_ok(req_id, "PONG", echo=request.get("payload"))
        elif req_type == "FAIL":
            return make_error(req_id, ErrorCodes.BAD_REQUEST, "Simulated failure")
        return make_error(req_id, ErrorCodes.BAD_REQUEST, f"Unknown command: {req_type}")


class TestStubServerAndClient:
    """Test client-server request-response exchange over actual loopback socket."""

    def test_client_server_request_response(self):
        server = StubEchoServer(host="127.0.0.1", port=0, name="StubServer")
        bound_port = server.start()

        try:
            with TCPClient(host="127.0.0.1", port=bound_port) as client:
                # 1. Test successful request
                req = {"type": "PING", "req_id": "req-001", "payload": "distributed-systems"}
                resp = client.send_request(req)
                assert resp["type"] == "PONG"
                assert resp["req_id"] == "req-001"
                assert resp["echo"] == "distributed-systems"

                # 2. Test error response
                fail_req = {"type": "FAIL", "req_id": "req-002"}
                err_resp = client.send_request(fail_req)
                assert err_resp["type"] == Commands.ERROR
                assert err_resp["error_code"] == ErrorCodes.BAD_REQUEST
                assert "Simulated failure" in err_resp["message"]
        finally:
            server.stop()

