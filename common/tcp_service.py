"""Reusable TCP Server and Client abstractions for PulserCore nodes."""

import socket
import threading
from typing import Any, Callable, Dict, Optional, Tuple

from common.framing import read_frame, write_frame
from common.logger import setup_logger


class BaseTCPServer:
    """Thread-per-connection TCP server with graceful lifecycle management."""

    def __init__(self, host: str, port: int, name: str = "TCPServer"):
        self.host = host
        self.port = port
        self.name = name
        self.logger = setup_logger(name)

        self._server_sock: Optional[socket.socket] = None
        self._is_running = threading.Event()
        self._listener_thread: Optional[threading.Thread] = None
        self._active_connections: set[socket.socket] = set()
        self._conn_lock = threading.Lock()

    def start(self) -> int:
        """Starts the server and begins listening for connections.
        
        Returns the bound port (useful when port=0 for dynamic binding).
        """
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(128)
        self.port = self._server_sock.getsockname()[1]

        self._is_running.set()
        self._listener_thread = threading.Thread(
            target=self._listen_loop,
            name=f"{self.name}-Listener",
            daemon=True
        )
        self._listener_thread.start()
        self.logger.info(f"Started and listening on {self.host}:{self.port}")
        return self.port

    def _listen_loop(self) -> None:
        """Main accept loop."""
        while self._is_running.is_set():
            try:
                client_sock, client_addr = self._server_sock.accept()
            except OSError:
                # Socket closed during shutdown
                break

            with self._conn_lock:
                self._active_connections.add(client_sock)

            worker = threading.Thread(
                target=self._client_handler_wrapper,
                args=(client_sock, client_addr),
                name=f"{self.name}-Client-{client_addr[1]}",
                daemon=True
            )
            worker.start()

    def _client_handler_wrapper(self, client_sock: socket.socket, client_addr: Tuple[str, int]) -> None:
        """Manages the lifecycle of a single client connection."""
        try:
            self.handle_connection(client_sock, client_addr)
        except Exception as e:
            if self._is_running.is_set():
                self.logger.exception(f"Unhandled error handling client {client_addr}: {e}")
        finally:
            with self._conn_lock:
                self._active_connections.discard(client_sock)
            try:
                client_sock.close()
            except Exception:
                pass

    def handle_connection(self, client_sock: socket.socket, client_addr: Tuple[str, int]) -> None:
        """Default request-response loop.
        
        Subclasses can override either this method or handle_request.
        """
        while self._is_running.is_set():
            frame = read_frame(client_sock)
            if frame is None:
                break  # Client disconnected cleanly

            response = self.handle_request(frame, client_sock)
            if response is not None:
                write_frame(client_sock, response)

    def handle_request(self, request: Dict[str, Any], client_sock: socket.socket) -> Optional[Dict[str, Any]]:
        """Handles a single parsed request and optionally returns a response.
        
        Must be overridden by child servers (e.g. StorageServer, BrokerServer).
        """
        raise NotImplementedError("Subclasses must implement handle_request")

    def stop(self) -> None:
        """Gracefully shuts down the server and closes all active client sockets."""
        if not self._is_running.is_set():
            return

        self.logger.info("Shutting down server...")
        self._is_running.clear()

        # Close listener socket to unblock accept()
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass

        # Close all active connections
        with self._conn_lock:
            for conn in list(self._active_connections):
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
            self._active_connections.clear()

        if self._listener_thread and self._listener_thread.is_alive():
            self._listener_thread.join(timeout=2.0)
        self.logger.info("Server stopped.")


class TCPClient:
    """Robust synchronous client for interacting with PulserCore servers."""

    def __init__(self, host: str, port: int, timeout: float = 10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None

    def connect(self) -> None:
        """Establishes connection to the target server."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))

    def send_request(self, request: Dict[str, Any], timeout: Optional[float] = None) -> Dict[str, Any]:
        """Sends a request frame and synchronously waits for the response frame."""
        if not self.sock:
            raise ConnectionError("Client is not connected. Call connect() first.")

        if timeout is not None:
            self.sock.settimeout(timeout)
        else:
            self.sock.settimeout(self.timeout)

        write_frame(self.sock, request)
        response = read_frame(self.sock)
        if response is None:
            raise ConnectionError("Server closed connection while waiting for response")
        return response

    def send_frame(self, data: Dict[str, Any]) -> None:
        """Sends a single frame without waiting for an immediate response."""
        if not self.sock:
            raise ConnectionError("Client is not connected.")
        write_frame(self.sock, data)

    def read_frame(self, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Reads a single incoming frame."""
        if not self.sock:
            raise ConnectionError("Client is not connected.")
        if timeout is not None:
            self.sock.settimeout(timeout)
        return read_frame(self.sock)

    def close(self) -> None:
        """Closes the client socket connection."""
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def __enter__(self) -> "TCPClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

