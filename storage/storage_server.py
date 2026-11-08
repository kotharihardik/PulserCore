"""Storage TCP Server exposing append, read, and last_offset APIs."""

from typing import Any, Dict, Optional
import socket

from common.protocol import Commands, ErrorCodes, make_error, make_ok
from common.tcp_service import BaseTCPServer
from storage.log_manager import FencedError, LogManager


class StorageServer(BaseTCPServer):
    """Network-facing Storage Service node."""

    def __init__(self, host: str, port: int, data_dir: str, name: str = "StorageService"):
        super().__init__(host, port, name=name)
        self.log_manager = LogManager(data_dir)

    def handle_request(self, request: Dict[str, Any], client_sock: socket.socket) -> Optional[Dict[str, Any]]:
        cmd = request.get("type")
        req_id = request.get("req_id")

        if cmd == Commands.APPEND:
            topic = request.get("topic")
            payload = request.get("payload")
            fencing_token = request.get("fencing_token")

            if topic is None or payload is None or fencing_token is None:
                return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic, payload, or fencing_token")

            try:
                offset = self.log_manager.append(topic, payload, int(fencing_token))
                return make_ok(req_id, Commands.APPEND_OK, topic=topic, offset=offset)
            except FencedError as e:
                return make_error(req_id, ErrorCodes.FENCED, str(e))
            except Exception as e:
                return make_error(req_id, ErrorCodes.INTERNAL_ERROR, str(e))

        elif cmd == Commands.READ:
            topic = request.get("topic")
            from_offset = request.get("from_offset", 0)
            max_count = request.get("max_count", 100)

            if topic is None:
                return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic")

            try:
                messages, next_offset = self.log_manager.read(topic, int(from_offset), int(max_count))
                return make_ok(
                    req_id,
                    Commands.READ_OK,
                    topic=topic,
                    messages=messages,
                    next_offset=next_offset
                )
            except Exception as e:
                return make_error(req_id, ErrorCodes.INTERNAL_ERROR, str(e))

        elif cmd == Commands.LAST_OFFSET:
            topic = request.get("topic")
            if topic is None:
                return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic")

            try:
                offset = self.log_manager.last_offset(topic)
                return make_ok(req_id, Commands.LAST_OFFSET_OK, topic=topic, last_offset=offset)
            except Exception as e:
                return make_error(req_id, ErrorCodes.INTERNAL_ERROR, str(e))

        return make_error(req_id, ErrorCodes.BAD_REQUEST, f"Unknown storage command: {cmd}")

    def stop(self) -> None:
        """Stops the TCP server and closes all storage files."""
        super().stop()
        self.log_manager.close()

