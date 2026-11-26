"""Storage TCP Server exposing append, read, and last_offset APIs."""

from typing import Any, Dict, Optional
import socket

from common.cursor import Cursor
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

        elif cmd == Commands.SAVE_CURSOR:
            topic = request.get("topic")
            subscription = request.get("subscription")
            fencing_token = request.get("fencing_token")
            mark_delete_offset = request.get("mark_delete_offset", -1)
            individual_acks = set(request.get("individual_acks", []))

            if not topic or not subscription or fencing_token is None:
                return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic, subscription, or fencing_token")

            try:
                cursor = Cursor(mark_delete_position=int(mark_delete_offset), individual_acks=individual_acks)
                self.log_manager.save_cursor(topic, subscription, cursor, int(fencing_token))
                return make_ok(req_id, Commands.SAVE_CURSOR_OK, topic=topic, subscription=subscription)
            except FencedError as e:
                return make_error(req_id, ErrorCodes.FENCED, str(e))
            except Exception as e:
                return make_error(req_id, ErrorCodes.INTERNAL_ERROR, str(e))

        elif cmd == Commands.LOAD_CURSOR:
            topic = request.get("topic")
            subscription = request.get("subscription")

            if not topic or not subscription:
                return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic or subscription")

            try:
                cursor = self.log_manager.load_cursor(topic, subscription)
                return make_ok(
                    req_id,
                    Commands.LOAD_CURSOR_OK,
                    topic=topic,
                    subscription=subscription,
                    mark_delete_offset=cursor.mark_delete_position,
                    individual_acks=sorted(list(cursor.individual_acks))
                )
            except Exception as e:
                return make_error(req_id, ErrorCodes.INTERNAL_ERROR, str(e))

        elif cmd == Commands.SET_RETENTION:
            topic = request.get("topic")
            if not topic:
                return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic")

            try:
                self.log_manager.set_retention_policy(
                    topic,
                    retention_time_sec=request.get("retention_time_sec"),
                    retention_size_bytes=request.get("retention_size_bytes")
                )
                return make_ok(req_id, Commands.SET_RETENTION_OK, topic=topic)
            except Exception as e:
                return make_error(req_id, ErrorCodes.INTERNAL_ERROR, str(e))

        elif cmd == Commands.APPLY_RETENTION:
            topic = request.get("topic")
            if not topic:
                return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic")

            try:
                stats = self.log_manager.apply_retention(
                    topic,
                    retention_time_sec=request.get("retention_time_sec"),
                    retention_size_bytes=request.get("retention_size_bytes"),
                    now=request.get("now")
                )
                return make_ok(req_id, Commands.APPLY_RETENTION_OK, topic=topic, **stats)
            except Exception as e:
                return make_error(req_id, ErrorCodes.INTERNAL_ERROR, str(e))

        return make_error(req_id, ErrorCodes.BAD_REQUEST, f"Unknown storage command: {cmd}")

    def stop(self) -> None:
        """Stops the TCP server and closes all storage files."""
        super().stop()
        self.log_manager.close()

