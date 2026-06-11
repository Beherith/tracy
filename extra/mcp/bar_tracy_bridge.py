# -*- coding: utf-8 -*-
"""
BAR + Tracy Bridge MCP Server

Sits between an AI client (Copilot, Claude, etc.) and two MCP servers:
  - BAR MCP (raw TCP JSON-RPC on 127.0.0.1:23452)
  - Tracy MCP (SSE/HTTP on 127.0.0.1:47380)

Exposes all BAR tools as standard MCP tools so any MCP client can use them
without needing raw-TCP support.

Transport to AI client: stdio (default) or SSE via FastMCP --transport flag.

Usage:
    # stdio mode (for MCP clients that spawn processes)
    python extra/mcp/bar_tracy_bridge.py

    # SSE mode (for long-running server)
    python extra/mcp/bar_tracy_bridge.py --transport sse

    # SSE on custom host/port
    python extra/mcp/bar_tracy_bridge.py --transport sse --host 127.0.0.1 --port 47381

Environment variables:
    BAR_MCP_HOST      - BAR MCP hostname  (default: 127.0.0.1)
    BAR_MCP_PORT      - BAR MCP port      (default: 23452)
    TRACY_MCP_HOST    - Tracy MCP hostname (default: 127.0.0.1)
    TRACY_MCP_PORT    - Tracy MCP port    (default: 47380)
    BRIDGE_LOG_LEVEL  - Log level         (default: INFO)
"""

from __future__ import annotations

import argparse
import inspect
import json
import logging
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set

_HERE = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_LOG_LEVEL = os.environ.get("BRIDGE_LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("bar_tracy_bridge")
logger.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))

formatter = logging.Formatter("[%(asctime)s] [BRIDGE] %(levelname)-5s %(message)s", datefmt="%H:%M:%S")

_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.setFormatter(formatter)
logger.addHandler(_stderr_handler)

_file_handler = logging.FileHandler(os.path.join(_HERE, "bar_tracy_bridge.log"), encoding="utf-8")
_file_handler.setFormatter(formatter)
logger.addHandler(_file_handler)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BAR_HOST = os.environ.get("BAR_MCP_HOST", "127.0.0.1")
BAR_PORT = int(os.environ.get("BAR_MCP_PORT", "23452"))

TRACY_HOST = os.environ.get("TRACY_MCP_HOST", "127.0.0.1")
TRACY_PORT = int(os.environ.get("TRACY_MCP_PORT", "47380"))
TRACY_ENGINE_HOST = os.environ.get("TRACY_ENGINE_HOST", "127.0.0.1")
TRACY_ENGINE_PORT = int(os.environ.get("TRACY_ENGINE_PORT", "8086"))
TRACY_ENGINE_ALIAS = os.environ.get("TRACY_ENGINE_ALIAS", "live_engine")
_TRACY_MCP_SCRIPT = os.path.join(_HERE, "tracy_mcp.py")
_TRACY_MCP_PID_FILE = os.path.join(_HERE, "tracy_mcp.pid")


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off", ""}


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        logger.warning("Invalid float for %s=%r; using %.2f", name, value, default)
        return default


BRIDGE_STARTUP_BAR_PROBE = _env_flag("BRIDGE_STARTUP_BAR_PROBE", True)
BRIDGE_STARTUP_BAR_TIMEOUT = _env_float("BRIDGE_STARTUP_BAR_TIMEOUT", 2.0)

# ---------------------------------------------------------------------------
# Phase 1.1 - BarTcpClient
# ---------------------------------------------------------------------------


class BarConnectionError(Exception):
    """Raised when the bridge cannot connect to BAR MCP."""


class BarToolExecutionError(Exception):
    """Raised when BAR MCP returns an MCP tool execution error."""


class BarTcpClient:
    """Persistent TCP client for BAR MCP's raw-TCP JSON-RPC 2.0 protocol.

    Handles:
    - Connection lifecycle (connect / disconnect / reconnect)
    - Newline-delimited JSON-RPC framing
    - Exponential backoff on reconnect
    - Thread-safe message sending
    - Response demultiplexing by request ID (background reader thread)
    """

    def __init__(
        self,
        host: str = BAR_HOST,
        port: int = BAR_PORT,
        reconnect_max: int = 5,
        reconnect_base: float = 1.0,
        connect_timeout: float = 5.0,
    ):
        self._host = host
        self._port = port
        self._reconnect_max = reconnect_max
        self._reconnect_base = reconnect_base
        self._connect_timeout = connect_timeout

        self._sock: Optional[socket.socket] = None
        self._buffer = ""
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._rpc_lock = threading.RLock()
        self._request_id = 0

        # Response demultiplexing: request_id -> (Event, result_or_error)
        self._pending: Dict[int, threading.Event] = {}
        self._pending_results: Dict[int, Any] = {}
        self._reader_running = False
        self._reader_thread: Optional[threading.Thread] = None

        # Notification callbacks (for handling server->client notifications)
        self._notification_callbacks: List[callable] = []

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._sock is not None

    def on_notification(self, callback: callable) -> None:
        """Register a callback for incoming JSON-RPC notifications.

        Args:
            callback: Function that receives (method: str, params: dict)
        """
        self._notification_callbacks.append(callback)

    def connect(self, timeout: Optional[float] = None) -> None:
        """Establish a TCP connection to BAR MCP.

        Raises BarConnectionError if the connection cannot be established.
        """
        addr = f"{self._host}:{self._port}"
        logger.info("Connecting to BAR MCP on %s ...", addr)

        connect_timeout = self._connect_timeout if timeout is None else timeout

        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(connect_timeout)
            self._sock.connect((self._host, self._port))
            self._sock.settimeout(1.0)  # non-blocking reads for reader loop
            self._buffer = ""
            logger.info("Connected to BAR MCP on %s", addr)
        except OSError as exc:
            self._cleanup_sock()
            raise BarConnectionError(
                f"Cannot connect to BAR MCP on {addr} - "
                f"is the game running with dev mode enabled? "
                f"(Spring.Utilities.IsDevMode())  Detail: {exc}"
            ) from exc

        # Start background reader thread
        self._start_reader()

    def disconnect(self) -> None:
        """Close the TCP connection and stop the reader thread."""
        self._stop_reader()
        if self._sock:
            logger.info("Disconnecting from BAR MCP")
            self._cleanup_sock()

    def _cleanup_sock(self) -> None:
        try:
            if self._sock:
                self._sock.close()
        except OSError:
            pass
        self._sock = None

    # ------------------------------------------------------------------
    # Background reader thread
    # ------------------------------------------------------------------

    def _start_reader(self) -> None:
        """Start the background reader thread if not already running."""
        if self._reader_running and self._reader_thread and self._reader_thread.is_alive():
            return
        self._reader_running = True
        self._reader_thread = threading.Thread(
            target=self._reader_loop, daemon=True, name="bar-tcp-reader"
        )
        self._reader_thread.start()

    def _stop_reader(self) -> None:
        """Signal the reader thread to stop and wait for it."""
        self._reader_running = False
        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=5.0)
        self._reader_thread = None

    @staticmethod
    def _parse_jsonrpc_obj(obj: Any) -> Optional[Dict[str, Any]]:
        """Validate that a decoded JSON value is a valid JSON-RPC message dict."""
        if not isinstance(obj, dict):
            return None
        return obj

    def _deliver_response(self, msg: Dict[str, Any]) -> None:
        """Demultiplex a parsed JSON-RPC response by request ID."""
        resp_id = msg.get("id")
        if resp_id is not None:
            resp_id_int = int(resp_id)
            with self._lock:
                ev = self._pending.pop(resp_id_int, None)
                self._pending_results[resp_id_int] = msg
            if ev:
                ev.set()
                logger.debug("BAR TCP <- response id=%s delivered", resp_id)
            else:
                logger.warning(
                    "BAR TCP <- early/unsolicited response id=%s stored",
                    resp_id,
                )
        else:
            logger.debug("BAR TCP <- notification: %s", json.dumps(msg)[:100])
            method = msg.get("method", "")
            params = msg.get("params", {})
            for cb in self._notification_callbacks:
                try:
                    cb(method, params)
                except Exception as exc:
                    logger.warning("BAR notification callback error: %s", exc)

    def _reader_loop(self) -> None:
        """Background thread: read from socket, parse JSON-RPC objects, demultiplex by id.

        Uses json.JSONDecoder.raw_decode() for JSON-aware parsing instead of
        naive newline splitting.  This correctly handles responses whose string
        values contain literal newline characters (e.g. vfs_read returning
        multi-line source files).
        """
        logger.debug("BAR TCP reader thread started")
        decoder = json.JSONDecoder()
        MAX_BUFFER = 4 * 1024 * 1024  # 4 MB safety cap

        while self._reader_running:
            try:
                # --- Try to decode complete JSON objects from the buffer ---
                while self._reader_running and self._buffer:
                    # Skip leading whitespace
                    stripped_pos = 0
                    while stripped_pos < len(self._buffer) and self._buffer[stripped_pos] in ' \t\n\r':
                        stripped_pos += 1

                    if stripped_pos > 0:
                        self._buffer = self._buffer[stripped_pos:]

                    if not self._buffer:
                        break

                    try:
                        obj, end_pos = decoder.raw_decode(self._buffer)
                        # Successfully parsed a JSON object - consume it
                        self._buffer = self._buffer[end_pos:]

                        msg = self._parse_jsonrpc_obj(obj)
                        if msg is not None:
                            self._deliver_response(msg)
                        else:
                            logger.debug("BAR TCP <- non-dict JSON: %s", str(obj)[:100])
                    except json.JSONDecodeError:
                        # Incomplete JSON - need more data from the socket
                        break

                # --- Safety: discard buffer if it grew too large (malformed data) ---
                if len(self._buffer) > MAX_BUFFER:
                    logger.warning(
                        "BAR TCP buffer exceeded %d bytes - discarding (likely malformed data)",
                        MAX_BUFFER,
                    )
                    self._buffer = ""

                # --- Recv more data from the socket ---
                if self._sock:
                    try:
                        chunk = self._sock.recv(65536)
                    except socket.timeout:
                        pass  # normal - nothing to read right now
                        continue
                    except OSError:
                        break

                    if not chunk:
                        # Connection closed by peer
                        break

                    self._buffer += chunk.decode("utf-8", errors="replace")

                if not self._sock:
                    break

            except Exception as exc:
                logger.warning("BAR TCP reader thread error: %s", exc)
                break

        logger.debug("BAR TCP reader thread stopped")
        self._cleanup_sock()
        # Notify any pending waiters that the connection is gone
        with self._lock:
            pending = list(self._pending.items())
            self._pending.clear()
            for req_id, ev in pending:
                self._pending_results[req_id] = BarConnectionError(
                    "BAR MCP connection lost while waiting for response."
                )
                ev.set()

    # ------------------------------------------------------------------
    # Response waiting
    # ------------------------------------------------------------------

    def receive_response(self, request_id: int, timeout: float = 10.0) -> Dict[str, Any]:
        """Wait for the JSON-RPC response matching a specific request ID.

        Uses the background reader thread to demultiplex responses by ID.
        Raises on timeout, connection loss, or parse error.
        """
        if not self.connected:
            raise BarConnectionError("Not connected to BAR MCP")

        return self._wait_for_response(request_id, timeout)

    def _wait_for_response(self, request_id: int, timeout: float) -> Dict[str, Any]:
        """Wait for the response matching a specific request ID.

        Blocks until the reader thread delivers the matching response or
        the timeout expires.
        """
        with self._lock:
            result = self._pending_results.pop(request_id, None)
            if result is not None:
                if isinstance(result, BarConnectionError):
                    raise result
                return result

            ev = threading.Event()
            self._pending[request_id] = ev

        if not ev.wait(timeout=timeout):
            with self._lock:
                self._pending.pop(request_id, None)
            raise BarConnectionError(
                f"Timeout waiting for BAR MCP response id={request_id} after {timeout}s. "
                f"Is dbg_bar_mcp.lua loaded? Check game console for '[BARMCP]' messages."
            )

        with self._lock:
            result = self._pending_results.pop(request_id, None)
        if isinstance(result, BarConnectionError):
            raise result
        if result is None:
            raise BarConnectionError(
                f"No response received for BAR MCP request id={request_id}."
            )
        return result

    def reconnect(self) -> None:
        """Reconnect with exponential backoff indefinitely."""
        self._stop_reader()
        self._cleanup_sock()
        # Clear pending requests so old waiters don't block forever
        with self._lock:
            pending = list(self._pending.items())
            self._pending.clear()
            self._pending_results.clear()
            for req_id, ev in pending:
                self._pending_results[req_id] = BarConnectionError(
                    "Reconnecting - previous request cancelled."
                )
                ev.set()

        addr = f"{self._host}:{self._port}"
        attempt = 1
        while True:
            wait = min(self._reconnect_base * (2 ** (attempt - 1)), 60.0)
            logger.warning(
                "Reconnect attempt %d to BAR MCP on %s (waiting %.1fs) ...",
                attempt, addr, wait,
            )
            time.sleep(wait)

            try:
                self.connect()
                logger.info("Successfully reconnected to BAR MCP on %s", addr)
                return  # success
            except BarConnectionError as exc:
                logger.warning("Reconnect attempt %d failed: %s", attempt, exc)
                attempt += 1

    # ------------------------------------------------------------------
    # JSON-RPC messaging
    # ------------------------------------------------------------------

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def send_jsonrpc(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        request_id: Optional[int] = None,
    ) -> int:
        """Send a JSON-RPC 2.0 request to BAR MCP.

        Returns the request ID used (auto-incremented if not provided).
        """
        if not self.connected:
            raise BarConnectionError(
                "Not connected to BAR MCP - call connect() first. "
                "Is the game running with dev mode enabled?"
            )

        with self._send_lock:
            msg: Dict[str, Any] = {
                "jsonrpc": "2.0",
                "method": method,
            }
            if params is not None:
                msg["params"] = params
            if request_id is not None:
                msg["id"] = request_id
            else:
                msg["id"] = self._next_id()
                request_id = msg["id"]

            payload = json.dumps(msg) + "\n"
            logger.debug("[BRIDGE:TCP_WIRE] -> method='%s' params type=%s value=%s", method, type(params).__name__, params)
            logger.debug("BAR TCP -> %s", payload.rstrip())

            try:
                if self._sock:
                    self._sock.sendall(payload.encode("utf-8"))
            except OSError as exc:
                self._cleanup_sock()
                raise BarConnectionError(
                    f"Lost connection to BAR MCP while sending - {exc}"
                ) from exc

        return request_id

    def send_notification(self, method: str, params: Optional[Dict[str, Any]] = None) -> None:
        """Send a JSON-RPC 2.0 notification (no id, no response expected)."""
        if not self.connected:
            raise BarConnectionError("Not connected to BAR MCP")

        msg: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
        }
        if params is not None:
            msg["params"] = params

        payload = json.dumps(msg) + "\n"
        logger.debug("BAR TCP -> (notification) %s", payload.rstrip())

        with self._send_lock:
            try:
                if self._sock:
                    self._sock.sendall(payload.encode("utf-8"))
            except OSError as exc:
                self._cleanup_sock()
                raise BarConnectionError(
                    f"Lost connection to BAR MCP while sending notification - {exc}"
                ) from exc

    # ------------------------------------------------------------------
    # High-level helpers
    # ------------------------------------------------------------------

    def call_method(
        self, method: str, params: Optional[Dict[str, Any]] = None, timeout: float = 30.0
    ) -> Dict[str, Any]:
        """Send a JSON-RPC request and wait for the matching response.

        Responses are matched by request ID via the background reader thread,
        so out-of-order or stray responses are handled correctly.

        Returns the parsed JSON-RPC response dict.
        Raises BarConnectionError on transport or protocol errors.
        """
        with self._rpc_lock:
            req_id = self.send_jsonrpc(method, params)
            response = self.receive_response(req_id, timeout)
            logger.debug("BAR TCP <- response id=%s", response.get("id"))
            return response

    def call_tool(self, tool_name: str, arguments: Optional[Dict[str, Any]] = None, timeout: float = 120.0) -> Dict[str, Any]:
        """Call a BAR MCP tool via `tools/call` and return the result.

        Returns the raw result dict from BAR (contains 'content' and 'isError').
        Raises BarToolExecutionError if the tool returned isError=true.

        Default timeout is 120s because the Lua server processes requests in
        widget:Update() which runs at game framerate - if the game is slow or
        paused, large responses (e.g. vfs_read of big files) can take a while.
        """
        params: Dict[str, Any] = {"name": tool_name}
        if arguments:
            params["arguments"] = arguments
        logger.debug("[BRIDGE:TCP_CLIENT] call_tool tool='%s' params=%s", tool_name, params)

        response = self.call_method("tools/call", params, timeout)

        if "error" in response:
            raise BarConnectionError(
                f"BAR MCP JSON-RPC error for '{tool_name}': "
                f"{response['error'].get('message', 'unknown')}"
            )

        result = response.get("result", {})
        if result.get("isError"):
            text = self._extract_text(result)
            raise BarToolExecutionError(
                f"BAR MCP tool '{tool_name}' returned error: {text} - "
                f"check game console for details."
            )

        return result

    def ping(self, timeout: float = 5.0) -> bool:
        """Send a heartbeat ping to BAR MCP and verify response."""
        try:
            # We call the 'ping' tool we just added to dbg_bar_mcp.lua
            result = self.call_tool("ping", timeout=timeout)
            return True
        except (BarConnectionError, BarToolExecutionError) as exc:
            logger.warning("BAR MCP heartbeat failed: %s", exc)
            return False

    @staticmethod
    def _extract_text(result: Dict[str, Any]) -> str:
        """Extract plain text from an MCP tool result."""
        contents = result.get("content", [])
        texts = []
        for item in contents:
            if item.get("type") == "text":
                texts.append(item.get("text", ""))
        return "\n".join(texts) if texts else "(empty result)"


# ---------------------------------------------------------------------------
# Phase 1.2 - BAR Tool Discovery & Forwarding
# ---------------------------------------------------------------------------


class BarToolRegistry:
    """Discovers BAR tools via `tools/list` and provides callable wrappers.

    Each discovered tool is exposed as a method that sends the call via TCP
    and returns the plain text result.
    """

    def __init__(self, client: BarTcpClient):
        self._client = client
        self._tools: List[Dict[str, Any]] = []
        self._tool_map: Dict[str, Dict[str, Any]] = {}

    @property
    def tools(self) -> List[Dict[str, Any]]:
        return list(self._tools)

    @property
    def tool_names(self) -> List[str]:
        return list(self._tool_map.keys())

    def discover(self, timeout: float = 15.0) -> List[Dict[str, Any]]:
        """Query BAR MCP for the list of available tools.

        Sends `tools/list` and caches the result.
        """
        logger.info("Discovering BAR tools ...")
        response = self._client.call_method("tools/list", None, timeout)

        if "error" in response:
            raise BarConnectionError(
                f"BAR MCP tools/list error: {response['error'].get('message', 'unknown')}"
            )

        result = response.get("result", {})
        self._tools = result.get("tools", [])
        self._tool_map = {t["name"]: t for t in self._tools}

        logger.info(
            "Discovered %d BAR tools: %s",
            len(self._tools),
            ", ".join(t["name"] for t in self._tools),
        )
        return self._tools

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None, timeout: float = 120.0) -> str:
        """Call a BAR tool by name and return the plain text result.

        Args:
            name: Tool name (e.g., "game_info", "widget_reload")
            arguments: Dict of arguments matching the tool's inputSchema
            timeout: Max seconds to wait for response (default 120s because
                     the Lua server processes requests in widget:Update() at
                     game framerate - if paused or slow, responses can take
                     a long time)

        Returns:
            Plain text result string from the tool.
        """
        logger.debug("[BRIDGE:REGISTRY] call_tool name='%s' arguments type=%s value=%s", name, type(arguments).__name__, arguments)
        if name not in self._tool_map:
            available = ", ".join(self._tool_map.keys())
            raise BarConnectionError(
                f"Unknown BAR tool '{name}'. Available: {available}. "
                f"Call discover() to refresh the tool list."
            )

        raw_result = self._client.call_tool(name, arguments, timeout)
        return BarTcpClient._extract_text(raw_result)


def _preview_text(value: Any, max_chars: int = 4096) -> str:
    text = str(value)
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}\n[BRIDGE: log truncated, chars={len(text)}]"


# ---------------------------------------------------------------------------
# Phase 2 - Tracy Integration
# ---------------------------------------------------------------------------


class TracyConnectionError(Exception):
    """Raised when the bridge cannot connect to Tracy MCP."""


class TracyHttpClient:
    """Proper SSE client for Tracy MCP's FastMCP SSE transport.

    MCP SSE transport model:
    1. Client opens a persistent GET /sse stream.
    2. Server sends an 'endpoint' event with the message POST URL.
    3. Client POSTs JSON-RPC messages to that URL.
    4. Server sends responses back on the SSE stream as 'result' events.
    5. Responses are demultiplexed by JSON-RPC request ID.

    This client maintains:
    - A persistent SSE stream (background reader thread).
    - Unique auto-incrementing request IDs.
    - Response demultiplexing by ID (thread-safe, like BarTcpClient).
    """

    def __init__(
        self,
        host: str = TRACY_HOST,
        port: int = TRACY_PORT,
        timeout: float = 30.0,
    ):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._base_url = f"http://{host}:{port}"
        self._connected = False

        # Connection lock to prevent concurrent reconnection attempts
        self._conn_lock = threading.Lock()

        # SSE session state
        self._message_url: Optional[str] = None  # POST endpoint from 'endpoint' event
        self._endpoint_event = threading.Event()  # signaled when endpoint received
        self._request_id = 0

        # Persistent SSE stream
        self._sse_stream = None  # httpx.Response (streaming)
        self._sse_context = None  # httpx._GeneratorContextManager (for cleanup)
        self._sse_reader_running = False
        self._sse_reader_thread: Optional[threading.Thread] = None

        # Response demultiplexing: request_id -> threading.Event
        self._lock = threading.Lock()
        self._pending: Dict[int, threading.Event] = {}
        self._pending_results: Dict[int, Any] = {}

        # Notification callbacks (for handling server->client notifications)
        self._notification_callbacks: List[callable] = []

    def on_notification(self, callback: callable) -> None:
        """Register a callback for incoming JSON-RPC notifications.

        Args:
            callback: Function that receives (method: str, params: dict)
        """
        self._notification_callbacks.append(callback)

    @property
    def connected(self) -> bool:
        return self._connected and self._message_url is not None

    @property
    def base_url(self) -> str:
        return self._base_url

    def connect(self) -> None:
        """Establish SSE session with Tracy MCP.

        Opens /sse, reads the 'endpoint' event to get the message URL,
        then starts the background SSE reader thread.

        Raises TracyConnectionError if the handshake fails.
        """
        addr = f"{self._host}:{self._port}"
        logger.info("Connecting to Tracy MCP on %s ...", addr)

        try:
            import httpx
        except ImportError:
            raise TracyConnectionError(
                "httpx is required for Tracy MCP communication. "
                "Install with: pip install httpx"
            )

        # Import httpx once and cache it
        self._httpx = httpx
        self._endpoint_event.clear()
        self._message_url = None

        # Open persistent SSE stream (httpx.stream returns a context manager)
        try:
            self._sse_context = self._httpx.stream(
                "GET",
                f"{self._base_url}/sse",
                timeout=self._timeout,
                headers={"Accept": "text/event-stream"},
            )
            # Enter the context manager to get the actual Response object
            self._sse_stream = self._sse_context.__enter__()
        except TracyConnectionError:
            raise
        except Exception as exc:
            raise TracyConnectionError(
                f"Cannot connect to Tracy MCP on {addr} - "
                f"is Tracy MCP runningus Check that tracy_mcp.py exists and "
                f"TracyServerBindings are built. Detail: {exc}"
            ) from exc

        # Start background SSE reader thread (it will detect the endpoint event)
        self._start_sse_reader()

        # Wait for the endpoint event from the reader thread
        if not self._endpoint_event.wait(timeout=self._timeout):
            self._stop_sse_reader()
            self._cleanup_sse()
            raise TracyConnectionError(
                f"Tracy MCP SSE handshake failed - no 'endpoint' event received within {self._timeout}s. "
                f"Is Tracy MCP running on {addr}us"
            )

        if not self._message_url:
            self._stop_sse_reader()
            self._cleanup_sse()
            raise TracyConnectionError(
                f"Tracy MCP SSE handshake failed - endpoint event had no URL. "
                f"Is Tracy MCP running on {addr}us"
            )

        # Mark connected now so send_request() passes its self.connected check
        # for the MCP initialize handshake below.
        self._connected = True

        # Make sure message_url is absolute
        if not self._message_url.startswith("http"):
            self._message_url = f"{self._base_url}{self._message_url}"

        # MCP handshake: initialize request, then initialized notification
        try:
            init_params = {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "bar_tracy_bridge", "version": "1.0.0"},
            }
            init_response = self.send_request("initialize", init_params, timeout=5.0)

            if "error" in init_response:
                logger.warning(
                    "Tracy MCP initialize error: %s",
                    init_response["error"].get("message", "unknown"),
                )
            else:
                logger.info(
                    "Tracy MCP initialized (protocol: %s)",
                    init_response.get("result", {}).get("protocolVersion"),
                )

            # Send initialized notification (no response expected)
            self._send_notification("notifications/initialized")
        except TracyConnectionError as exc:
            logger.warning("Tracy MCP initialize failed (non-fatal): %s", exc)

        logger.info("Connected to Tracy MCP on %s (endpoint: %s)", addr, self._message_url)

    @staticmethod
    def _parse_sse_event(text: str) -> tuple:
        """Parse an SSE event block into (event_type, data).

        SSE format:
            event: <type>\n
            data: <json>\n
            \n

        Multi-line data is supported per the SSE spec: consecutive ``data:``
        lines are joined with newlines into a single payload.
        """
        event_type = "message"  # default event type
        data_lines: list[str] = []

        for line in text.split("\n"):
            if line.startswith("event: "):
                event_type = line[7:].strip()
            elif line.startswith("data: "):
                data_lines.append(line[6:])
            elif line.startswith("data"):
                # edge case: ``data:value`` (no space after colon)
                data_lines.append(line[4:])

        return event_type, "\n".join(data_lines)

    def disconnect(self) -> None:
        """Close the SSE session and stop the reader thread."""
        if self._connected:
            logger.info("Disconnecting from Tracy MCP")
        self._stop_sse_reader()
        self._cleanup_sse()
        self._connected = False
        self._message_url = None

    def reconnect(self) -> None:
        """Attempt to reconnect to Tracy MCP indefinitely."""
        attempt = 1
        while True:
            try:
                with self._conn_lock:
                    self.disconnect()
                    self.connect()
                logger.info("Successfully reconnected to Tracy MCP")
                return
            except TracyConnectionError as exc:
                wait = min(1.0 * (2 ** (attempt - 1)), 60.0)
                logger.warning("Tracy MCP reconnect attempt %d failed: %s (waiting %.1fs)", attempt, exc, wait)
                time.sleep(wait)
                attempt += 1

    def _cleanup_sse(self) -> None:
        """Close the SSE stream and clean up resources."""
        if self._sse_context:
            try:
                self._sse_context.__exit__(None, None, None)
            except Exception:
                pass
            self._sse_context = None
        self._sse_stream = None

    # ------------------------------------------------------------------
    # Background SSE reader thread
    # ------------------------------------------------------------------

    def _start_sse_reader(self) -> None:
        """Start the background SSE reader thread."""
        if self._sse_reader_running and self._sse_reader_thread and self._sse_reader_thread.is_alive():
            return
        self._sse_reader_running = True
        self._sse_reader_thread = threading.Thread(
            target=self._sse_reader_loop, daemon=True, name="tracy-sse-reader"
        )
        self._sse_reader_thread.start()

    def _stop_sse_reader(self) -> None:
        """Signal the SSE reader thread to stop and wait for it."""
        self._sse_reader_running = False
        if self._sse_reader_thread and self._sse_reader_thread.is_alive():
            self._sse_reader_thread.join(timeout=5.0)
        self._sse_reader_thread = None

    def _sse_reader_loop(self) -> None:
        """Background thread: read SSE events and demultiplex responses by ID."""
        logger.debug("Tracy SSE reader thread started")

        if not self._sse_stream:
            logger.error("No SSE stream available for reader")
            return

        buffer = ""
        try:
            for line in self._sse_stream.iter_lines():
                if not self._sse_reader_running:
                    break

                buffer += line + "\n"

                # Parse complete SSE events
                while "\n\n" in buffer:
                    event_text, _, buffer = buffer.partition("\n\n")
                    event_type, data = self._parse_sse_event(event_text)

                    if event_type == "endpoint":
                        # First endpoint event - store URL and signal connect()
                        if not self._message_url:
                            self._message_url = data.strip()
                            logger.debug("Tracy SSE -> endpoint event: %s", self._message_url)
                            self._endpoint_event.set()
                        else:
                            logger.debug("Tracy SSE -> duplicate endpoint: %s", data)
                    elif event_type in ("message", "result"):
                        # FastMCP (MCP Python SDK) sends JSON-RPC responses as
                        # 'event: message'.  Handle both 'message' and 'result'
                        # for forward compatibility.
                        logger.debug("Tracy SSE <- %s event, data=%s", event_type, data[:200])
                        self._handle_sse_response(data)
                    elif event_type == "error":
                        logger.warning("Tracy SSE -> error event: %s", data[:200])
                    else:
                        logger.debug("Tracy SSE -> event [%s]: %s", event_type, data[:100])

        except Exception as exc:
            logger.warning("Tracy SSE reader thread error: %s", exc)
        finally:
            self._connected = False
            # Notify any pending waiters that the connection is gone.
            with self._lock:
                pending = list(self._pending.items())
                self._pending.clear()
                for req_id, ev in pending:
                    self._pending_results[req_id] = TracyConnectionError(
                        "Tracy MCP SSE connection lost while waiting for response."
                    )
                    ev.set()
            logger.debug("Tracy SSE reader thread stopped")

    def _handle_sse_response(self, data: str) -> None:
        """Parse an SSE 'result' event and deliver to the waiting request."""
        if not data.strip():
            return
        try:
            msg = json.loads(data)
        except json.JSONDecodeError:
            logger.error("Tracy SSE -> invalid JSON: %s", data[:200])
            return

        if not isinstance(msg, dict):
            logger.warning("Tracy SSE -> non-dict response: %s", data[:100])
            return

        resp_id = msg.get("id")
        if resp_id is not None:
            with self._lock:
                ev = self._pending.pop(int(resp_id), None)
                if ev:
                    self._pending_results[int(resp_id)] = msg
                    ev.set()
                    logger.debug("Tracy SSE <- response id=%s delivered", resp_id)
                else:
                    self._pending_results[int(resp_id)] = msg
                    logger.debug(
                        "Tracy SSE <- early/unsolicited response id=%s stored",
                        resp_id,
                    )
        else:
            # Notification (no id) - dispatch to callbacks
            method = msg.get("method", "unknown")
            params = msg.get("params", {})
            logger.debug("Tracy SSE <- notification: method=%s", method)
            for cb in self._notification_callbacks:
                try:
                    cb(method, params)
                except Exception as cb_exc:
                    logger.warning("Tracy notification callback error: %s", cb_exc)

    # ------------------------------------------------------------------
    # JSON-RPC messaging
    # ------------------------------------------------------------------

    def _next_id(self) -> int:
        """Generate the next unique request ID."""
        self._request_id += 1
        return self._request_id

    def _wait_for_response(self, request_id: int, timeout: float) -> Dict[str, Any]:
        """Wait for the SSE response matching a specific request ID."""
        with self._lock:
            result = self._pending_results.pop(request_id, None)
            if result is not None:
                if isinstance(result, TracyConnectionError):
                    raise result
                return result

            ev = self._pending.get(request_id)
            if ev is None:
                ev = threading.Event()
                self._pending[request_id] = ev

        if not ev.wait(timeout=timeout):
            with self._lock:
                self._pending.pop(request_id, None)
            raise TracyConnectionError(
                f"Timeout waiting for Tracy MCP response id={request_id} after {timeout}s."
            )

        with self._lock:
            result = self._pending_results.pop(request_id, None)
        if isinstance(result, TracyConnectionError):
            raise result
        if result is None:
            raise TracyConnectionError(
                f"No response received for Tracy MCP request id={request_id}."
            )
        return result

    def send_request(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Send a JSON-RPC request to Tracy MCP and wait for SSE response.

        POSTs to the message endpoint, then waits for the matching 'result'
        event on the SSE stream.

        Returns the parsed JSON-RPC response dict.
        Raises TracyConnectionError on transport or protocol errors.
        """
        if not self.connected:
            raise TracyConnectionError(
                "Not connected to Tracy MCP - call connect() first. "
                "Is Tracy MCP runningus"
            )

        with self._lock:
            req_id = self._next_id()
            self._pending[req_id] = threading.Event()
        msg: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
            "id": req_id,
        }
        if params is not None:
            msg["params"] = params

        req_timeout = timeout or self._timeout

        logger.debug("Tracy SSE -> POST id=%s method=%s", req_id, method)

        try:
            # POST to the message endpoint (fire-and-forget, response comes via SSE)
            resp = self._httpx.post(
                self._message_url,
                json=msg,
                timeout=req_timeout,
            )

            if resp.status_code not in (200, 202):
                logger.error(
                    "Tracy SSE POST failed with status %d: %s",
                    resp.status_code, resp.text[:200],
                )
                # Don't raise yet - the response might still arrive via SSE
                # Fall through to wait_for_response

        except Exception as exc:
            logger.warning("Tracy SSE POST error: %s", exc)
            # Fall through to wait_for_response - might still arrive

        # Wait for response on SSE stream
        return self._wait_for_response(req_id, req_timeout)

    def _send_notification(self, method: str, params: Optional[Dict[str, Any]] = None) -> None:
        """Send a JSON-RPC 2.0 notification to Tracy MCP (no id, no response expected)."""
        if not self.connected:
            raise TracyConnectionError("Not connected to Tracy MCP")

        msg: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
        }
        if params is not None:
            msg["params"] = params

        logger.debug("Tracy SSE -> (notification) %s", json.dumps(msg)[:200])

        try:
            self._httpx.post(
                self._message_url,
                json=msg,
                timeout=self._timeout,
            )
        except Exception as exc:
            logger.warning("Tracy SSE notification POST error: %s", exc)

    def discover_tools(self, timeout: float = 15.0) -> List[Dict[str, Any]]:
        """Query Tracy MCP for the list of available tools.

        Returns the raw tools list from the MCP `tools/list` response.
        """
        logger.info("Discovering Tracy MCP tools ...")
        response = self.send_request("tools/list", None, timeout)

        if "error" in response:
            raise TracyConnectionError(
                f"Tracy MCP tools/list error: "
                f"{response['error'].get('message', 'unknown')}"
            )

        result = response.get("result", {})
        tools = result.get("tools", [])
        logger.info(
            "Discovered %d Tracy MCP tools: %s",
            len(tools),
            ", ".join(t.get("name", "us") for t in tools),
        )
        return tools

    def call_tool(
        self,
        tool_name: str,
        arguments: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Call a Tracy MCP tool and return the result.

        Args:
            tool_name: Tool name (e.g., "eval", "list_instances")
            arguments: Dict of arguments for the tool
            timeout: Override timeout for this request

        Returns:
            The tool's result (parsed from JSON-RPC response)
        """
        params = {"name": tool_name}
        if arguments:
            params["arguments"] = arguments

        response = self.send_request("tools/call", params, timeout)

        if "error" in response:
            raise TracyConnectionError(
                f"Tracy MCP error for '{tool_name}': "
                f"{response['error'].get('message', 'unknown')}"
            )

        return response.get("result")


class TracyToolRegistry:
    """Discovers Tracy MCP tools via `tools/list` for internal bridge use.

    The bridge deliberately does not expose raw Tracy MCP tools to the client.
    Profiling is the public surface; discovered Tracy tools stay behind the
    supervisor so file/capture utilities do not clutter MCP tool lists.
    """

    def __init__(self, client: TracyHttpClient):
        self._client = client
        self._tools: List[Dict[str, Any]] = []
        self._tool_map: Dict[str, Dict[str, Any]] = {}

    @property
    def tools(self) -> List[Dict[str, Any]]:
        return list(self._tools)

    @property
    def tool_names(self) -> List[str]:
        return list(self._tool_map.keys())

    def discover(self, timeout: float = 15.0) -> List[Dict[str, Any]]:
        """Query Tracy MCP for the list of available tools.

        Sends `tools/list` and caches the result.
        """
        logger.info("Discovering Tracy MCP tools ...")
        tools = self._client.discover_tools(timeout)
        self._tools = tools
        self._tool_map = {t["name"]: t for t in self._tools}
        logger.info(
            "Cached %d Tracy MCP tools: %s",
            len(self._tools),
            ", ".join(t["name"] for t in self._tools),
        )
        return self._tools

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None, timeout: float = 120.0) -> Any:
        """Call a Tracy tool by name and return the raw result.

        Args:
            name: Tool name (e.g., "eval", "list_instances")
            arguments: Dict of arguments matching the tool's inputSchema
            timeout: Max seconds to wait for response

        Returns:
            Raw result dict from Tracy MCP.
        """
        logger.debug("[BRIDGE:TRACY_REG] call_tool name='%s' arguments=%s", name, arguments)
        if name not in self._tool_map:
            available = ", ".join(self._tool_map.keys())
            raise TracyConnectionError(
                f"Unknown Tracy tool '{name}'. Available: {available}. "
                f"Call discover() to refresh the tool list."
            )

        return self._client.call_tool(name, arguments, timeout)


class TracyAutoStart:
    """Manages Tracy MCP lifecycle: check, start, wait, auto-connect.

    On initialization:
    1. Checks if Tracy MCP is running (PID file)
    2. If not, spawns tracy_mcp.py as subprocess
    3. Waits for SSE endpoint to be ready
    4. Optionally auto-connects to a live Tracy server
    """

    def __init__(
        self,
        script_path: str = _TRACY_MCP_SCRIPT,
        pid_file: str = _TRACY_MCP_PID_FILE,
        host: str = TRACY_HOST,
        port: int = TRACY_PORT,
    ):
        self._script_path = script_path
        self._pid_file = pid_file
        self._host = host
        self._port = port
        self._process: Optional[subprocess.Popen] = None
        self._started_by_us = False

    @property
    def process(self) -> Optional[subprocess.Popen]:
        return self._process

    def _is_running(self) -> bool:
        """Check if Tracy MCP is running via TCP port liveness check."""
        try:
            with socket.create_connection((self._host, self._port), timeout=1.0):
                return True
        except (OSError, socket.timeout):
            return False

    def _wait_for_endpoint(self, timeout: float = 30.0) -> bool:
        """Poll the SSE endpoint until ready or timeout."""
        import httpx

        deadline = time.monotonic() + timeout
        url = f"http://{self._host}:{self._port}/sse"

        while time.monotonic() < deadline:
            try:
                with httpx.stream(
                    "GET",
                    url,
                    timeout=2.0,
                    headers={"Accept": "text/event-stream"},
                ) as response:
                    if response.status_code == 200:
                        return True
            except Exception:
                pass
            time.sleep(0.5)

        return False

    def ensure_running(self, auto_start: bool = True) -> bool:
        """Ensure Tracy MCP is running.

        Args:
            auto_start: If True, start Tracy MCP if not running.

        Returns:
            True if Tracy MCP is running, False otherwise.
        """
        addr = f"{self._host}:{self._port}"

        if self._is_running():
            return True

        if not auto_start:
            logger.warning(
                "Tracy MCP not running and auto_start=False. "
                "Tracy tools will not be available."
            )
            return False

        # Auto-start Tracy MCP
        if not os.path.exists(self._script_path):
            logger.error(
                "Tracy MCP script not found at %s. "
                "Cannot auto-start.", self._script_path
            )
            return False

        logger.info("Starting Tracy MCP ...")
        try:
            python = sys.executable or "python3"
            self._process = subprocess.Popen(
                [python, self._script_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                cwd=os.path.dirname(self._script_path),
            )
            self._started_by_us = True
            logger.info("Tracy MCP started (PID: %d)", self._process.pid)
        except Exception as exc:
            raise TracyConnectionError(
                f"Failed to start Tracy MCP: {exc}. "
                f"Check that tracy_mcp.py exists at {self._script_path} "
                f"and TracyServerBindings are built."
            ) from exc

        # Wait for SSE endpoint
        if not self._wait_for_endpoint():
            # Kill the process
            if self._process:
                self._process.terminate()
                try:
                    self._process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self._process.kill()
            raise TracyConnectionError(
                f"Tracy MCP failed to start within 30s. "
                f"Check that tracy_mcp.py exists at {self._script_path} "
                f"and TracyServerBindings are built. "
                f"Check stderr output for errors."
            )

        logger.info("Tracy MCP is ready on %s", addr)
        return True

    def auto_connect(
        self,
        client: TracyHttpClient,
        address: str = "127.0.0.1",
        port: int = 8086,
        alias: Optional[str] = None,
    ) -> Optional[str]:
        """Auto-connect Tracy MCP to a live Tracy server.

        Args:
            client: Connected TracyHttpClient instance
            address: Tracy server address
            port: Tracy server port (broadcast port)
            alias: Optional instance name

        Returns:
            Instance ID if successful, None if connection failed.
        """
        if not client.connected:
            logger.warning("Tracy MCP not connected, cannot auto-connect")
            return None

        logger.info(
            "Auto-connecting Tracy MCP to engine at %s:%d ...", address, port
        )

        try:
            result = client.call_tool(
                "live_connect",
                {"address": address, "port": port, "alias": alias},
                timeout=15.0,
            )

            # call_tool returns the full MCP result dict:
            #   {"content": [{"type": "text", "text": "..."}], "isError": false}
            # Extract the plain text string from it.
            if isinstance(result, dict):
                content_list = result.get("content", [])
                text_parts = [
                    item.get("text", "")
                    for item in content_list
                    if isinstance(item, dict) and item.get("type") == "text"
                ]
                text_result = "\n".join(text_parts) if text_parts else str(result)
            else:
                text_result = str(result) if result is not None else ""

            if text_result.startswith("Error"):
                logger.warning("Tracy live_connect failed: %s", text_result)
                return None

            # The text contains: "Connected to live instance as 'live_engine'. ..."
            # Extract the instance alias from the response.
            match = re.search(r"as '([^']+)'", text_result)
            instance_id = match.group(1) if match else "live_engine"
            logger.info("Tracy MCP connected to engine: %s", text_result[:100])
            return instance_id

        except TracyConnectionError as exc:
            import traceback
            logger.warning(
                "Tracy live_connect failed - exception details:"
            )
            logger.warning(
                "  Exception type    : %s",
                type(exc).__name__,
            )
            logger.warning(
                "  Exception message : %s",
                str(exc),
            )
            logger.warning(
                "  Exception args    : %s",
                exc.args,
            )
            logger.warning(
                "  Connection target : %s:%d",
                address, port,
            )
            logger.warning(
                "  Instance alias    : %s",
                alias,
            )
            logger.warning(
                "  Client connected  : %s",
                client.connected if client else False,
            )
            if client and hasattr(client, "_message_url"):
                logger.warning(
                    "  SSE message URL   : %s",
                    client._message_url,
                )
            logger.warning(
                "  Traceback         :\n%s",
                "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            )
            logger.warning(
                "  Hint              : is the engine built with TRACY_ENABLE? "
                "Is Tracy MCP running and healthyus"
            )
            return None

    def cleanup(self) -> None:
        """Clean up resources (don't kill Tracy MCP - it may be shared)."""
        # We don't kill Tracy MCP on exit since it may be used by other tools
        # The PID file cleanup is handled by tracy_mcp.py itself
        pass


# ---------------------------------------------------------------------------
# Phase 3 - Profile Tools
# ---------------------------------------------------------------------------


class ProfileCollector:
    """Orchestrates optional reload, profiling wait, and Tracy zone collection."""

    def __init__(
        self,
        bar_registry: BarToolRegistry,
        tracy_client: TracyHttpClient,
        tracy_instance_id: str,
    ):
        self._bar = bar_registry
        self._tracy = tracy_client
        self._instance_id = tracy_instance_id

    def _tracy_eval(self, code: str, timeout: float = 60.0) -> str:
        """Execute Python code against the Tracy Worker via eval tool."""
        result = self._tracy.call_tool(
            "eval",
            {"code": code, "instance_id": self._instance_id},
            timeout=timeout,
        )
        if isinstance(result, dict):
            if result.get("isError"):
                raise TracyConnectionError(
                    f"Tracy eval returned an error: {self._extract_mcp_text(result)}"
                )
            return self._extract_mcp_text(result)
        return str(result) if result is not None else ""

    @staticmethod
    def _extract_mcp_text(result: Dict[str, Any]) -> str:
        contents = result.get("content", [])
        texts = [
            item.get("text", "")
            for item in contents
            if isinstance(item, dict) and item.get("type") == "text"
        ]
        return "\n".join(texts) if texts else str(result)

    def _get_zone_stats_snapshot(self) -> Dict[str, Any]:
        """Capture a snapshot of all zone stats keyed by source-location ID."""
        code = """
import re
result = {}
for key, stats in ctx.get_all_zone_stats().items():
    m = re.search(r'<(\\d+)>$', key)
    if m:
        sid = m.group(1)
        result[sid] = {
            'count': stats.count,
            'total': stats.total,
            'min': stats.min,
            'max': stats.max,
            'avg': stats.avg,
        }
import json
json.dumps(result)
"""
        raw = self._tracy_eval(code, timeout=60.0)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Tracy eval returned non-JSON for zone snapshot: %s", raw[:200])
            return {}

    def _get_zone_names_by_pattern(self, zone_pattern: str) -> Dict[str, str]:
        """Get source-location IDs and display names matching a Python regex."""
        re.compile(zone_pattern)
        code = f"""
import re
zone_re = re.compile({zone_pattern!r})
result = {{}}
seen = set()
for key, stats in ctx.get_all_zone_stats().items():
    display = key.split(' (')[0] if ' (' in key else key
    if not (zone_re.search(display) or zone_re.search(key)):
        continue
    m = re.search(r'<(\\d+)>$', key)
    if m:
        sid = m.group(1)
        if sid not in seen:
            seen.add(sid)
            result[sid] = display
import json
json.dumps(result)
"""
        raw = self._tracy_eval(code, timeout=60.0)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Tracy eval returned non-JSON for zone names: %s", raw[:200])
            return {}

    def _reload_and_profile(
        self,
        zone_pattern: str,
        duration: float = 5.0,
        reload_tool: Optional[str] = None,
        reload_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Profile zones matching a regex, optionally reloading code first."""
        if duration < 0:
            raise ValueError("duration must be >= 0")
        if reload_tool and not reload_name:
            raise ValueError("reload_name is required when reload_tool is set")

        logger.info(
            "Starting profile: zone_pattern=%r, reload=%s(%r), duration=%.1fs",
            zone_pattern,
            reload_tool or "none",
            reload_name,
            duration,
        )
        before = self._get_zone_stats_snapshot()
        logger.debug("Before snapshot: %d zones captured", len(before))

        if reload_tool:
            logger.info("Reloading via BAR: %s(name=%r)", reload_tool, reload_name)
            try:
                reload_result = self._bar.call_tool(reload_tool, {"name": reload_name}, timeout=15.0)
                logger.info("BAR reload result: %s", reload_result[:200])
            except BarConnectionError as exc:
                raise BarConnectionError(
                    f"Failed to reload {reload_name} via BAR MCP: {exc}"
                ) from exc

        logger.info("Waiting %.1fs for zone data to accumulate", duration)
        time.sleep(duration)
        after = self._get_zone_stats_snapshot()
        logger.debug("After snapshot: %d zones captured", len(after))

        relevant_zones = self._get_zone_names_by_pattern(zone_pattern)
        result = self._compute_delta(before, after, zone_pattern, relevant_zones)
        result["reload"] = {"tool": reload_tool, "name": reload_name} if reload_tool else None
        return result

    @staticmethod
    def _compute_delta(
        before: Dict[str, Any],
        after: Dict[str, Any],
        zone_pattern: str,
        relevant_zones: Dict[str, str],
    ) -> Dict[str, Any]:
        """Compute the delta between two zone stat snapshots."""
        zones: Dict[str, Dict[str, Any]] = {}

        for sid, display_name in relevant_zones.items():
            b = before.get(sid, {})
            a = after.get(sid, {})
            b_count = b.get("count", 0)
            a_count = a.get("count", 0)
            if a_count <= b_count:
                continue
            zones[sid] = {
                "name": display_name,
                "count": a_count - b_count,
                "total": a.get("total", 0) - b.get("total", 0),
                "min": a.get("min", 0),
                "max": a.get("max", 0),
                "avg": a.get("avg", 0),
            }

        result = {
            "zone_pattern": zone_pattern,
            "zone_count": len(zones),
            "zones": zones,
        }
        if not zones:
            logger.warning(
                "No Tracy zones found with new entries after profiling pattern %r. "
                "Did you instrument matching tracy.ZoneBeginN(...) / tracy.ZoneEnd() zonesus",
                zone_pattern,
            )
        return result

    def profile_zone_pattern(
        self,
        zone_pattern: str,
        duration: float = 5.0,
        reload_tool: Optional[str] = None,
        reload_name: Optional[str] = None,
    ) -> str:
        """Profile Tracy zones matching a regex, optionally after a BAR reload."""
        result = self._reload_and_profile(zone_pattern, duration, reload_tool, reload_name)
        return self._format_result(result)

    def profile_diff(
        self,
        zone_pattern: str,
        duration: float = 5.0,
        reload_tool: Optional[str] = None,
        reload_name: Optional[str] = None,
    ) -> str:
        """Run two profile passes and return the delta."""
        logger.info(
            "Starting diff profile: zone_pattern=%r, reload=%s(%r), duration=%.1fs",
            zone_pattern,
            reload_tool or "none",
            reload_name,
            duration,
        )
        pass1 = self._reload_and_profile(zone_pattern, duration, reload_tool, reload_name)
        time.sleep(0.5)
        pass2 = self._reload_and_profile(zone_pattern, duration, reload_tool, reload_name)
        delta = self._compute_pass_delta(pass1, pass2, zone_pattern)
        delta["reload"] = {"tool": reload_tool, "name": reload_name} if reload_tool else None
        return self._format_diff_result(delta)

    @staticmethod
    def _compute_pass_delta(
        pass1: Dict[str, Any],
        pass2: Dict[str, Any],
        zone_pattern: str,
    ) -> Dict[str, Any]:
        """Compute the delta between two profiling passes."""
        zones1 = pass1.get("zones", {})
        zones2 = pass2.get("zones", {})
        delta_zones: Dict[str, Dict[str, Any]] = {}

        for sid in set(zones1.keys()) | set(zones2.keys()):
            z1 = zones1.get(sid, {})
            z2 = zones2.get(sid, {})
            c1 = z1.get("count", 0)
            c2 = z2.get("count", 0)
            t1 = z1.get("total", 0)
            t2 = z2.get("total", 0)
            count_diff = c2 - c1
            total_diff = t2 - t1
            count_pct = (count_diff / c1 * 100) if c1 else 0
            total_pct = (total_diff / t1 * 100) if t1 else 0
            delta_zones[sid] = {
                "name": z2.get("name") or z1.get("name"),
                "count_before": c1,
                "count_after": c2,
                "count_diff": count_diff,
                "count_pct": round(count_pct, 1),
                "total_before": t1,
                "total_after": t2,
                "total_diff": total_diff,
                "total_pct": round(total_pct, 1),
                "avg_before": z1.get("avg", 0),
                "avg_after": z2.get("avg", 0),
            }

        return {
            "zone_pattern": zone_pattern,
            "zone_count": len(delta_zones),
            "zones": delta_zones,
        }

    @staticmethod
    def _format_result(result: Dict[str, Any]) -> str:
        """Format profile result as a readable string with JSON data."""
        zone_pattern = result["zone_pattern"]
        zone_count = result["zone_count"]
        zones = result["zones"]
        reload_info = result.get("reload")

        lines = [
            f"Profile result for zone pattern {zone_pattern!r}:",
            f"  Zones with new entries: {zone_count}",
        ]
        if reload_info:
            lines.append(f"  Reload: {reload_info['tool']}({reload_info['name']!r})")
        lines.append("")

        if zone_count:
            lines.append("  Zone stats (count, total_us, min_us, max_us, avg_us):")
            sorted_zones = sorted(
                zones.items(), key=lambda kv: kv[1].get("total", 0), reverse=True
            )
            for sid, stats in sorted_zones[:50]:
                total_us = stats["total"] / 1e3
                min_us = stats["min"] / 1e3
                max_us = stats["max"] / 1e3
                avg_us = stats["avg"] / 1e3
                lines.append(
                    f"    [{sid}] count={stats['count']:>6}  "
                    f"total={total_us:>10.2f}  "
                    f"min={min_us:>8.2f}  "
                    f"max={max_us:>8.2f}  "
                    f"avg={avg_us:>8.2f}  "
                    f"name={stats.get('name', '')}"
                )
            if len(zones) > 50:
                lines.append(f"    ... and {len(zones) - 50} more zones (see JSON below)")
            lines.append("")

        lines.append("  Full JSON:")
        lines.append(json.dumps(result, indent=2))
        return "\n".join(lines)

    @staticmethod
    def _format_diff_result(result: Dict[str, Any]) -> str:
        """Format diff profile result as a readable string."""
        zone_pattern = result["zone_pattern"]
        zone_count = result["zone_count"]
        zones = result["zones"]
        reload_info = result.get("reload")

        lines = [
            f"Diff profile result for zone pattern {zone_pattern!r}:",
            f"  Zones compared: {zone_count}",
        ]
        if reload_info:
            lines.append(f"  Reload: {reload_info['tool']}({reload_info['name']!r})")
        lines.append("")

        if zone_count:
            lines.append("  Zone delta (count_diff, total_diff_us, total_pct, avg_before_us, avg_after_us):")
            sorted_zones = sorted(
                zones.items(),
                key=lambda kv: abs(kv[1].get("total_pct", 0)),
                reverse=True,
            )
            for sid, stats in sorted_zones[:50]:
                total_diff_us = stats["total_diff"] / 1e3
                avg_b_us = stats["avg_before"] / 1e3
                avg_a_us = stats["avg_after"] / 1e3
                direction = "up" if stats["total_diff"] > 0 else "down" if stats["total_diff"] < 0 else "flat"
                lines.append(
                    f"    [{sid}] {direction} count={stats['count_diff']:>+6}  "
                    f"total_diff={total_diff_us:>10.2f}  "
                    f"pct={stats['total_pct']:>+7.1f}%  "
                    f"avg={avg_b_us:>8.2f} -> {avg_a_us:>8.2f}  "
                    f"name={stats.get('name', '')}"
                )
            if len(zones) > 50:
                lines.append(f"    ... and {len(zones) - 50} more zones (see JSON below)")
            lines.append("")

        lines.append("  Full JSON:")
        lines.append(json.dumps(result, indent=2))
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Phase 4 - Main Server Entrypoint
# ---------------------------------------------------------------------------


def create_bridge_server() -> Any:
    """Create the FastMCP server instance with basic configuration."""
    import mcp.server.fastmcp as fastmcp

# Ok this prompt on how to profile with tracy must be fully complete and detailed, because it's the main value prop of this tool and we want to make sure users understand how to use it effectively. We also want to set expectations about what profiling is suitable for (repeated code paths, not one-shot init/shutdown code), and how to interpret results. The instructions should be clear enough that even users new to Tracy can get started with profiling their BAR Lua code using this MCP bridge.
    server = fastmcp.FastMCP(name = "Beyond All Reason + Tracy profiling MCP server",
    instructions = """
# Beyond All Reason + Tracy Profiling MCP Server

This server exposes tools for the game Beyond All Reason (BAR) on the Recoil Engine (SpringRTS) for developing and profiling BAR LuaUI widgets and LuaRules gadgets, with deep integration to Tracy for performance insights.

Tracy profiling should be done by adding searchable zones in the Lua code, e.g. tracy.ZoneBeginN("MyWidget:Update") / tracy.ZoneEnd(), then calling profile_zone_pattern with a regex such as "^MyWidget:".

Example:

'''lua 
function foo(bar)
    tracy.ZoneBeginN("MyWidget:foo") -- start a zone with a custom name (appears in Tracy UI)
    
    -- do work here  
    
    if bar > 0 then
        tracy.ZoneEnd() -- end the zone before any early return
        return true
    end
    tracy.ZoneEnd() -- make sure to end the zone on all code paths
end
'''
## Important Guidelines for instrumenting zones:
- Do not localize the tracy.ZoneBeginN / ZoneEnd calls - they must be global, always use tracy.ZoneBeginN(...) and tracy.ZoneEnd() directly
- Only put tracy zones within functions, do not place them in the top levels of the script.
- Always ensure that every tracy.ZoneBeginN(...) is paired with a tracy.ZoneEnd() on all code paths, including error paths. Unmatched zones can lead to incorrect profiling data and potential memory leaks in Tracy.
- Do not profile single-shot code that runs once at startup or shutdown, unless specifically asked, such as:
    - Initialize(), Shutdown(), Initialize(), Shutdown()
- Focus on code that runs repeatedly during gameplay, such as:
    - Update(), MousePress(), GameFrame(), etc.
- Functions such as `function widget:Update()` are called every frame, so they are prime candidates for profiling. Instrumenting them with zones allows you to see how much time is spent in each part of the update logic across frames.
- Some functions are already pre-instrumented, such as `widget:GameFrame()`, with the zone naming: "W:GameFrame:MyWidget" for widget code and "G:GameFrame:MyGadget" for gadget code.
 
    """)
    return server


def _schema_type_to_python_type(schema_type: str) -> Any:
    if schema_type == "string":
        return str
    if schema_type == "integer":
        return int
    if schema_type == "number":
        return float
    if schema_type == "boolean":
        return bool
    if schema_type == "array":
        return list
    if schema_type == "object":
        return dict
    return Any


def _signature_from_input_schema(input_schema: Dict[str, Any]) -> inspect.Signature:
    properties = input_schema.get("properties", {})
    if not isinstance(properties, dict):
        properties = {}
    required = set(input_schema.get("required", []))

    params = []
    for name, prop in properties.items():
        if not isinstance(prop, dict):
            prop = {}
        annotation = _schema_type_to_python_type(prop.get("type", "string"))
        default = inspect.Parameter.empty
        if name not in required:
            default = prop.get("default", None)
        params.append(
            inspect.Parameter(
                name,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=default,
                annotation=annotation,
            )
        )

    params.sort(key=lambda p: p.default is not inspect.Parameter.empty)
    return inspect.Signature(params, return_annotation=str)


def _compact_tool_args(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Drop omitted optional args while preserving explicit falsey values."""
    return {k: v for k, v in kwargs.items() if v is not None}



BRIDGE_TOOL_NAMES = {
    "bridge_status",
    "bridge_reconnect",
    "bar_call_tool",
    "bar_refresh_tools",
    "bridge_dynamic_tools",
    "profile_zone_pattern",
    "profile_zone_pattern_diff",
}


def _parse_tool_arguments(arguments: Any) -> Dict[str, Any]:
    if arguments is None:
        return {}
    if isinstance(arguments, dict):
        return arguments
    if isinstance(arguments, str):
        if not arguments.strip():
            return {}
        parsed = json.loads(arguments)
        if not isinstance(parsed, dict):
            raise ValueError("Tool arguments JSON must decode to an object.")
        return parsed
    raise ValueError("Tool arguments must be an object or a JSON object string.")


def _mcp_result_to_text(result: Any) -> str:
    if isinstance(result, dict):
        contents = result.get("content", [])
        texts = [
            item.get("text", "")
            for item in contents
            if isinstance(item, dict) and item.get("type") == "text"
        ]
        if texts:
            return "\n".join(texts)
    return json.dumps(result) if isinstance(result, (dict, list)) else str(result) if result is not None else ""


def _mcp_result_to_data(result: Any) -> Any:
    if isinstance(result, dict):
        structured = result.get("structuredContent")
        if structured is not None:
            if isinstance(structured, dict) and "result" in structured:
                return structured["result"]
            return structured
        text = _mcp_result_to_text(result)
        try:
            return json.loads(text)
        except (TypeError, json.JSONDecodeError):
            return result
    return result


class ToolListNotifier:
    """Central best-effort hook for dynamic MCP tool-list changes."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._generation = 0
        self._sessions: Dict[int, Dict[str, Any]] = {}
        self._notification_thread_active = False

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation

    def status(self) -> Dict[str, Any]:
        with self._lock:
            pending = sum(1 for session in self._sessions.values() if session["seen_generation"] < self._generation)
            return {
                "generation": self._generation,
                "captured_sessions": len(self._sessions),
                "pending_sessions": pending,
            }

    def capture_request_context(self, request_context: Any, mark_generation_seen: bool = False) -> bool:
        """Remember the current MCP session so background threads can notify it."""
        session = getattr(request_context, "session", None)
        if session is None or not hasattr(session, "send_tool_list_changed"):
            return False
        try:
            from anyio.lowlevel import current_token

            token = current_token()
        except Exception as exc:
            logger.debug("Could not capture MCP event loop token for tool-list notifications: %s", exc)
            return False

        session_id = id(session)
        schedule = False
        with self._lock:
            previous = self._sessions.get(session_id)
            if mark_generation_seen:
                seen_generation = self._generation
            elif previous is not None:
                seen_generation = previous["seen_generation"]
            else:
                seen_generation = 0
            self._sessions[session_id] = {
                "session": session,
                "token": token,
                "seen_generation": seen_generation,
            }
            if not mark_generation_seen and self._generation > seen_generation:
                schedule = True
        if schedule:
            self._schedule_notifications()
        return True

    def notify(self, source: str, tool_names: List[str]) -> None:
        with self._lock:
            self._generation += 1
            generation = self._generation
        logger.info(
            "Tool list changed from %s (generation %d): %s",
            source,
            generation,
            ", ".join(tool_names),
        )
        self._schedule_notifications()

    def _schedule_notifications(self) -> None:
        with self._lock:
            if self._notification_thread_active:
                return
            self._notification_thread_active = True
        thread = threading.Thread(
            target=self._notification_loop,
            daemon=True,
            name="tool-list-change-notifier",
        )
        thread.start()

    def _notification_loop(self) -> None:
        try:
            while True:
                did_work = self._send_pending_notifications()
                with self._lock:
                    pending = any(
                        session["seen_generation"] < self._generation for session in self._sessions.values()
                    )
                    if not pending or not did_work:
                        self._notification_thread_active = False
                        return
        except Exception:
            logger.exception("Unexpected failure in tool-list notification loop")
            with self._lock:
                self._notification_thread_active = False

    def _send_pending_notifications(self) -> bool:
        with self._lock:
            generation = self._generation
            targets = [
                (session_id, session["session"], session["token"], session["seen_generation"])
                for session_id, session in self._sessions.items()
                if session["seen_generation"] < generation
            ]
        if not targets:
            logger.debug("Tool list changed, but no MCP client session has been captured yet")
            return False

        sent_ids: List[int] = []
        dead_ids: List[int] = []
        for session_id, session, token, _seen_generation in targets:
            try:
                from anyio import from_thread

                from_thread.run(session.send_tool_list_changed, token=token)
                sent_ids.append(session_id)
            except Exception as exc:
                logger.debug("Dropping MCP session %s after tool-list notification failed: %s", session_id, exc)
                dead_ids.append(session_id)

        with self._lock:
            for session_id in sent_ids:
                session = self._sessions.get(session_id)
                if session is not None:
                    session["seen_generation"] = max(session["seen_generation"], generation)
            for session_id in dead_ids:
                self._sessions.pop(session_id, None)
        if sent_ids:
            logger.info(
                "Sent tools/list_changed notification for generation %d to %d MCP client session(s)",
                generation,
                len(sent_ids),
            )
        return bool(sent_ids or dead_ids)


def _remove_tool(server: Any, name: str) -> bool:
    manager = getattr(server, "_tool_manager", None)
    tools = getattr(manager, "_tools", None)
    if isinstance(tools, dict) and name in tools:
        del tools[name]
        return True
    return False


def _install_or_replace_tool(server: Any, name: str, description: str, func: Callable[..., Any]) -> None:
    _remove_tool(server, name)
    server.tool(name=name, description=description)(func)


def _capture_current_mcp_session(server: Any, notifier: ToolListNotifier, mark_generation_seen: bool) -> None:
    mcp_server = getattr(server, "_mcp_server", None)
    if mcp_server is None:
        return
    try:
        request_context = mcp_server.request_context
    except LookupError:
        return
    except Exception as exc:
        logger.debug("Could not read current MCP request context: %s", exc)
        return
    notifier.capture_request_context(request_context, mark_generation_seen=mark_generation_seen)


def _enable_dynamic_tool_notifications(server: Any, notifier: ToolListNotifier) -> None:
    """Advertise and emit MCP tool-list change notifications when SDK hooks exist."""
    mcp_server = getattr(server, "_mcp_server", None)
    if mcp_server is None:
        logger.debug("FastMCP low-level server is unavailable; tool-list notifications disabled")
        return

    _enable_tool_list_changed_capability(mcp_server)
    _wrap_tool_request_handlers(server, notifier, mcp_server)


def _enable_tool_list_changed_capability(mcp_server: Any) -> None:
    original = getattr(mcp_server, "create_initialization_options", None)
    if not callable(original) or getattr(original, "_bar_tracy_tools_changed_enabled", False):
        return
    try:
        from mcp.server.lowlevel import NotificationOptions
    except Exception as exc:
        logger.debug("Could not import MCP NotificationOptions; listChanged capability disabled: %s", exc)
        return

    def create_initialization_options(
        notification_options: Any = None,
        experimental_capabilities: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Any:
        if notification_options is None:
            notification_options = NotificationOptions(tools_changed=True)
        elif hasattr(notification_options, "tools_changed"):
            notification_options.tools_changed = True
        return original(
            notification_options=notification_options,
            experimental_capabilities=experimental_capabilities,
        )

    create_initialization_options._bar_tracy_tools_changed_enabled = True  # type: ignore[attr-defined]
    setattr(mcp_server, "create_initialization_options", create_initialization_options)


def _wrap_tool_request_handlers(server: Any, notifier: ToolListNotifier, mcp_server: Any) -> None:
    handlers = getattr(mcp_server, "request_handlers", None)
    if not isinstance(handlers, dict):
        logger.debug("FastMCP request handlers are unavailable; tool-list notifications may be delayed")
        return
    try:
        import mcp.types as mcp_types
    except Exception as exc:
        logger.debug("Could not import MCP request types; tool-list session capture disabled: %s", exc)
        return

    list_handler = handlers.get(mcp_types.ListToolsRequest)
    if callable(list_handler) and not getattr(list_handler, "_bar_tracy_session_capture", False):

        async def captured_list_tools(req: Any, _original: Callable[..., Any] = list_handler) -> Any:
            _capture_current_mcp_session(server, notifier, mark_generation_seen=True)
            return await _original(req)

        captured_list_tools._bar_tracy_session_capture = True  # type: ignore[attr-defined]
        handlers[mcp_types.ListToolsRequest] = captured_list_tools

    call_handler = handlers.get(mcp_types.CallToolRequest)
    if callable(call_handler) and not getattr(call_handler, "_bar_tracy_session_capture", False):

        async def captured_call_tool(req: Any, _original: Callable[..., Any] = call_handler) -> Any:
            _capture_current_mcp_session(server, notifier, mark_generation_seen=False)
            return await _original(req)

        captured_call_tool._bar_tracy_session_capture = True  # type: ignore[attr-defined]
        handlers[mcp_types.CallToolRequest] = captured_call_tool


class BarBackendSupervisor:
    """Keeps the BAR TCP MCP backend reconnectable behind stable bridge tools."""

    def __init__(self, server: Any, notifier: ToolListNotifier, host: str = BAR_HOST, port: int = BAR_PORT) -> None:
        self._server = server
        self._notifier = notifier
        self._host = host
        self._port = port
        self._lock = threading.RLock()
        self._connect_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._installed_tools: Set[str] = set()
        self._backoff = 1.0
        self.client = BarTcpClient(host, port)
        self.registry = BarToolRegistry(self.client)
        self.state = "offline"
        self.generation = 0
        self.last_error: Optional[str] = None
        self.last_ready_at: Optional[float] = None

    def start(self) -> None:
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True, name="bar-backend-supervisor")
        self._monitor_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=2.0)
        self.client.disconnect()

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "state": self.state,
                "generation": self.generation,
                "tools": len(self.registry.tools),
                "tool_names": self.registry.tool_names,
                "last_error": self.last_error,
                "last_ready_at": self.last_ready_at,
                "target": f"{self._host}:{self._port}",
            }

    def _set_state(self, state: str, error: Optional[str] = None) -> None:
        with self._lock:
            if self.state != state:
                logger.info("BAR backend state: %s -> %s", self.state, state)
            self.state = state
            self.last_error = error

    def mark_offline(self, error: str) -> None:
        logger.warning("BAR backend marked offline: %s", error)
        self._set_state("offline", error)
        with self._lock:
            self.generation += 1
        self.client.disconnect()

    def ensure_ready(self, timeout: float = 5.0, connect_timeout: Optional[float] = None) -> None:
        with self._lock:
            if self.state == "ready" and self.client.connected:
                return
        if not self._connect_lock.acquire(timeout=timeout):
            raise BarConnectionError("Timed out waiting for BAR reconnect lock.")
        try:
            with self._lock:
                if self.state == "ready" and self.client.connected:
                    return
            self._set_state("connecting")
            self.client.disconnect()
            self.client.connect(timeout=connect_timeout)
            self._initialize_client()
            self.registry.discover()
            self._install_dynamic_tools()
            with self._lock:
                self.state = "ready"
                self.last_error = None
                self.generation += 1
                self.last_ready_at = time.time()
                self._backoff = 1.0
            logger.info("BAR backend ready (generation %d)", self.generation)
        except Exception as exc:
            self._set_state("offline", str(exc))
            self.client.disconnect()
            raise
        finally:
            self._connect_lock.release()

    def refresh_tools(self) -> List[str]:
        self.ensure_ready()
        before = set(self.registry.tool_names)
        self.registry.discover()
        self._install_dynamic_tools()
        after = set(self.registry.tool_names)
        if before != after:
            with self._lock:
                self.generation += 1
        return self.registry.tool_names

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None, timeout: float = 120.0) -> str:
        last_exc: Optional[Exception] = None
        for attempt in range(2):
            self.ensure_ready()
            if name not in self.registry.tool_names:
                self.refresh_tools()
            try:
                return self.registry.call_tool(name, arguments or {}, timeout=timeout)
            except BarToolExecutionError:
                raise
            except BarConnectionError as exc:
                last_exc = exc
                self.mark_offline(str(exc))
                if attempt == 0:
                    continue
                break
        raise BarConnectionError(f"BAR tool '{name}' failed after reconnect: {last_exc}")

    def _initialize_client(self) -> None:
        init_params = {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "bar_tracy_bridge", "version": "1.0.0"},
        }
        init_response = self.client.call_method("initialize", init_params, timeout=5.0)
        if "error" in init_response:
            logger.warning("BAR MCP initialize error: %s", init_response["error"].get("message", "unknown"))
        self.client.send_notification("notifications/initialized")

    def _install_dynamic_tools(self) -> None:
        discovered_names: Set[str] = set()
        for tool_def in self.registry.tools:
            tool_name = tool_def["name"]
            if tool_name in BRIDGE_TOOL_NAMES:
                logger.warning("Skipping BAR tool '%s' because it conflicts with a bridge tool", tool_name)
                continue
            discovered_names.add(tool_name)
            tool_desc = tool_def.get("description", "")
            input_schema = tool_def.get("inputSchema", {})
            func = self._make_tool_handler(tool_name, tool_desc, input_schema if isinstance(input_schema, dict) else {})
            _install_or_replace_tool(self._server, tool_name, tool_desc, func)
        for old_name in self._installed_tools - discovered_names:
            _remove_tool(self._server, old_name)
        if discovered_names != self._installed_tools:
            self._installed_tools = discovered_names
            self._notifier.notify("bar", sorted(discovered_names))

    def _make_tool_handler(self, tool_name: str, tool_desc: str, input_schema: Dict[str, Any]):
        def handler(**kwargs) -> str:
            return self.call_tool(tool_name, _compact_tool_args(kwargs), timeout=120.0)

        handler.__name__ = tool_name
        handler.__doc__ = tool_desc
        handler.__signature__ = _signature_from_input_schema(input_schema)
        handler.__annotations__ = {"return": str}
        return handler

    def _monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            wait = 5.0
            try:
                if self.state == "ready":
                    if not self.client.ping(timeout=2.0):
                        self.mark_offline("BAR heartbeat failed")
                else:
                    self.ensure_ready(timeout=2.0)
            except Exception as exc:
                self._set_state("offline", str(exc))
                wait = min(self._backoff, 30.0) + random.random()
                self._backoff = min(self._backoff * 2.0, 30.0)
            else:
                wait = 5.0 if self.state == "ready" else min(self._backoff, 30.0) + random.random()
            self._stop_event.wait(wait)


class TracyBackendSupervisor:
    """Keeps Tracy MCP and the live engine Tracy instance reconnectable."""

    def __init__(
        self,
        server: Any,
        notifier: ToolListNotifier,
        host: str = TRACY_HOST,
        port: int = TRACY_PORT,
        engine_address: str = "127.0.0.1",
        engine_port: int = 8086,
        engine_alias: str = "live_engine",
    ) -> None:
        self._server = server
        self._notifier = notifier
        self._host = host
        self._port = port
        self._engine_address = engine_address
        self._engine_port = engine_port
        self._engine_alias = engine_alias
        self._auto = TracyAutoStart(host=host, port=port)
        self._lock = threading.RLock()
        self._connect_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._installed_tools: Set[str] = set()
        self._backoff = 1.0
        self.client: Optional[TracyHttpClient] = None
        self.registry: Optional[TracyToolRegistry] = None
        self.mcp_state = "offline"
        self.engine_state = "offline"
        self.generation = 0
        self.engine_generation = 0
        self.instance_id: Optional[str] = None
        self.last_error: Optional[str] = None
        self.last_ready_at: Optional[float] = None

    def start(self) -> None:
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True, name="tracy-backend-supervisor")
        self._monitor_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=2.0)
        self._disconnect_client()

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "mcp_state": self.mcp_state,
                "engine_state": self.engine_state,
                "generation": self.generation,
                "engine_generation": self.engine_generation,
                "instance_id": self.instance_id,
                "tools": len(self.registry.tools) if self.registry else 0,
                "tool_names": self.registry.tool_names if self.registry else [],
                "last_error": self.last_error,
                "last_ready_at": self.last_ready_at,
                "target": f"{self._host}:{self._port}",
                "engine_target": f"{self._engine_address}:{self._engine_port}",
            }

    def ensure_mcp_ready(self, timeout: float = 30.0) -> None:
        with self._lock:
            if self.mcp_state == "ready" and self.client and self.client.connected and self.registry:
                return
        if not self._connect_lock.acquire(timeout=timeout):
            raise TracyConnectionError("Timed out waiting for Tracy reconnect lock.")
        try:
            with self._lock:
                if self.mcp_state == "ready" and self.client and self.client.connected and self.registry:
                    return
                self.mcp_state = "connecting"
                self.last_error = None
            self._disconnect_client()
            if not self._auto.ensure_running(auto_start=True):
                raise TracyConnectionError("Tracy MCP is not running and could not be auto-started.")
            client = TracyHttpClient(self._host, self._port)
            client.connect()
            registry = TracyToolRegistry(client)
            registry.discover()
            with self._lock:
                self.client = client
                self.registry = registry
                self.mcp_state = "ready"
                self.last_error = None
                self.generation += 1
                self.last_ready_at = time.time()
                self._backoff = 1.0
            self._install_dynamic_tools()
            logger.info("Tracy MCP backend ready (generation %d)", self.generation)
        except Exception as exc:
            self.mark_mcp_offline(str(exc))
            raise
        finally:
            self._connect_lock.release()

    def ensure_engine_ready(self) -> str:
        self.ensure_mcp_ready()
        with self._lock:
            if self.instance_id and self.engine_state == "ready" and self._instance_exists_locked(self.instance_id):
                return self.instance_id
            client = self.client
        if not client:
            raise TracyConnectionError("Tracy MCP client is unavailable.")
        with self._lock:
            self.engine_state = "connecting"
        instance_id = self._auto.auto_connect(client, address=self._engine_address, port=self._engine_port, alias=self._engine_alias)
        if not instance_id:
            with self._lock:
                self.engine_state = "offline"
                self.last_error = "Tracy MCP could not connect to the live engine."
            raise TracyConnectionError(f"Tracy MCP could not connect to the engine at {self._engine_address}:{self._engine_port}.")
        with self._lock:
            self.instance_id = instance_id
            self.engine_state = "ready"
            self.engine_generation += 1
            self.last_error = None
        return instance_id

    def refresh_tools(self) -> List[str]:
        self.ensure_mcp_ready()
        if not self.registry:
            return []
        before = set(self.registry.tool_names)
        self.registry.discover()
        self._install_dynamic_tools()
        after = set(self.registry.tool_names)
        if before != after:
            with self._lock:
                self.generation += 1
        return self.registry.tool_names

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None, timeout: float = 120.0) -> Any:
        last_exc: Optional[Exception] = None
        for attempt in range(2):
            self.ensure_mcp_ready()
            if not self.registry:
                raise TracyConnectionError("Tracy registry is unavailable.")
            if name not in self.registry.tool_names:
                self.refresh_tools()
            try:
                return self.registry.call_tool(name, arguments or {}, timeout=timeout)
            except TracyConnectionError as exc:
                last_exc = exc
                self.mark_mcp_offline(str(exc))
                if attempt == 0:
                    continue
                break
        raise TracyConnectionError(f"Tracy tool '{name}' failed after reconnect: {last_exc}")

    def mark_mcp_offline(self, error: str) -> None:
        logger.warning("Tracy MCP backend marked offline: %s", error)
        with self._lock:
            self.mcp_state = "offline"
            self.engine_state = "offline"
            self.instance_id = None
            self.last_error = error
            self.generation += 1
        self._disconnect_client()

    def mark_engine_offline(self, error: str) -> None:
        logger.warning("Tracy engine backend marked offline: %s", error)
        with self._lock:
            self.engine_state = "offline"
            self.instance_id = None
            self.last_error = error
            self.engine_generation += 1

    def _disconnect_client(self) -> None:
        client = self.client
        self.client = None
        self.registry = None
        if client:
            client.disconnect()

    def _instance_exists_locked(self, instance_id: str) -> bool:
        client = self.client
        if not client:
            return False
        try:
            result = client.call_tool("list_instances", {}, timeout=5.0)
            data = _mcp_result_to_data(result)
            if isinstance(data, list):
                return any(isinstance(item, dict) and item.get("id") == instance_id for item in data)
        except Exception as exc:
            logger.debug("Could not validate Tracy instance '%s': %s", instance_id, exc)
            return False
        return False

    def _install_dynamic_tools(self) -> None:
        for old_name in list(self._installed_tools):
            _remove_tool(self._server, old_name)
        if self._installed_tools:
            self._installed_tools = set()
            self._notifier.notify("tracy", [])

    def _monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            wait = 5.0
            try:
                if self.mcp_state != "ready":
                    self.ensure_mcp_ready(timeout=5.0)
                elif self.client and not self.client.connected:
                    self.mark_mcp_offline("Tracy SSE stream disconnected")
                elif self.instance_id and self.engine_state == "ready":
                    with self._lock:
                        exists = self._instance_exists_locked(self.instance_id)
                    if not exists:
                        self.mark_engine_offline("Tracy live engine instance disappeared")
            except Exception as exc:
                with self._lock:
                    self.last_error = str(exc)
                    if self.mcp_state != "ready":
                        self.mcp_state = "offline"
                wait = min(self._backoff, 30.0) + random.random()
                self._backoff = min(self._backoff * 2.0, 30.0)
            else:
                wait = 5.0 if self.mcp_state == "ready" else min(self._backoff, 30.0) + random.random()
            self._stop_event.wait(wait)


def _bridge_status_payload(bar_backend: BarBackendSupervisor, tracy_backend: TracyBackendSupervisor, notifier: ToolListNotifier) -> Dict[str, Any]:
    bar = bar_backend.status()
    tracy = tracy_backend.status()
    return {
        "bar": bar,
        "tracy_mcp": {
            "state": tracy["mcp_state"],
            "generation": tracy["generation"],
            "tools": tracy["tools"],
            "tool_names": tracy["tool_names"],
            "last_error": tracy["last_error"],
            "last_ready_at": tracy["last_ready_at"],
            "target": tracy["target"],
        },
        "tracy_engine": {
            "state": tracy["engine_state"],
            "generation": tracy["engine_generation"],
            "instance_id": tracy["instance_id"],
            "target": tracy["engine_target"],
        },
        "profile_tools_available": tracy["mcp_state"] == "ready" and tracy["engine_state"] == "ready",
        "profile_reload_available": bar["state"] == "ready" and tracy["mcp_state"] == "ready" and tracy["engine_state"] == "ready",
        "tool_notification_generation": notifier.generation,
        "tool_notifications": notifier.status(),
        "config": {
            "bar_target": f"{BAR_HOST}:{BAR_PORT}",
            "tracy_mcp_target": f"{TRACY_HOST}:{TRACY_PORT}",
            "tracy_engine_target": f"{TRACY_ENGINE_HOST}:{TRACY_ENGINE_PORT}",
            "tracy_engine_alias": TRACY_ENGINE_ALIAS,
        },
    }


def _run_profile_with_retry(
    bar_backend: BarBackendSupervisor,
    tracy_backend: TracyBackendSupervisor,
    notifier: ToolListNotifier,
    zone_pattern: str,
    duration: float,
    reload_kind: str = "",
    reload_name: str = "",
    diff: bool = False,
) -> str:
    reload_name = (reload_name or "").strip()
    reload_tool = _reload_tool_from_kind(reload_kind, reload_name)
    last_exc: Optional[Exception] = None
    for attempt in range(2):
        try:
            if reload_tool:
                bar_backend.ensure_ready()
            instance_id = tracy_backend.ensure_engine_ready()
            if not tracy_backend.client:
                raise TracyConnectionError("Tracy client is unavailable after reconnect.")
            collector = ProfileCollector(bar_backend.registry, tracy_backend.client, instance_id)
            if diff:
                return collector.profile_diff(zone_pattern, duration, reload_tool=reload_tool, reload_name=reload_name or None)
            return collector.profile_zone_pattern(zone_pattern, duration, reload_tool=reload_tool, reload_name=reload_name or None)
        except BarConnectionError as exc:
            last_exc = exc
            bar_backend.mark_offline(str(exc))
        except TracyConnectionError as exc:
            last_exc = exc
            tracy_backend.mark_engine_offline(str(exc))
        if attempt == 0:
            logger.info("Retrying profile pattern %r after backend reconnect", zone_pattern)
            continue
    status = _bridge_status_payload(bar_backend, tracy_backend, notifier)
    raise RuntimeError(f"Profile failed after reconnect: {last_exc}\n\nBridge status:\n{json.dumps(status, indent=2)}")


def _reload_tool_from_kind(reload_kind: str = "", reload_name: str = "") -> Optional[str]:
    kind = (reload_kind or "").strip().lower()
    name = (reload_name or "").strip()
    if kind in {"", "none", "no", "off", "skip", "false"}:
        if name:
            raise ValueError("reload_name was provided but reload_kind is empty; use 'widget' or 'gadget'.")
        return None
    if kind in {"widget", "widgets", "luaui"}:
        return "widget_reload"
    if kind in {"gadget", "gadgets", "luarules"}:
        return "gadget_reload"
    raise ValueError("reload_kind must be one of: '', 'none', 'widget', or 'gadget'.")


def register_stable_bridge_tools(server: Any, bar_backend: BarBackendSupervisor, tracy_backend: TracyBackendSupervisor, notifier: ToolListNotifier) -> None:
    @server.tool(description="Return BAR, Tracy MCP, Tracy engine, and dynamic tool discovery status.")
    def bridge_status() -> str:
        return json.dumps(_bridge_status_payload(bar_backend, tracy_backend, notifier), indent=2)

    @server.tool(description="List currently installed dynamic BAR convenience tools.")
    def bridge_dynamic_tools() -> str:
        status = _bridge_status_payload(bar_backend, tracy_backend, notifier)
        dynamic = {
            "bar": status["bar"]["tool_names"],
            "tracy": [],
            "notification_generation": status["tool_notification_generation"],
        }
        return json.dumps(dynamic, indent=2)

    @server.tool(description="Reconnect BAR and/or Tracy backends and refresh discovered tools.")
    def bridge_reconnect(bar: bool = True, tracy: bool = True, engine: bool = True) -> str:
        results: Dict[str, Any] = {}
        if bar:
            try:
                bar_backend.mark_offline("manual reconnect requested")
                bar_backend.ensure_ready()
                results["bar"] = "ready"
            except Exception as exc:
                results["bar"] = f"error: {exc}"
        if tracy:
            try:
                tracy_backend.mark_mcp_offline("manual reconnect requested")
                tracy_backend.ensure_mcp_ready()
                results["tracy_mcp"] = "ready"
            except Exception as exc:
                results["tracy_mcp"] = f"error: {exc}"
        if engine:
            try:
                results["tracy_engine"] = tracy_backend.ensure_engine_ready()
            except Exception as exc:
                results["tracy_engine"] = f"error: {exc}"
        results["status"] = _bridge_status_payload(bar_backend, tracy_backend, notifier)
        return json.dumps(results, indent=2)

    @server.tool(description="Call any discovered BAR MCP tool by name. Arguments may be an object or JSON object string.")
    def bar_call_tool(name: str, arguments: dict = None, timeout: float = 120.0) -> str:
        return bar_backend.call_tool(name, _parse_tool_arguments(arguments), timeout=timeout)

    @server.tool(description="Reconnect BAR if needed, refresh BAR tools, and update BAR convenience wrappers.")
    def bar_refresh_tools() -> str:
        names = bar_backend.refresh_tools()
        return f"BAR tools refreshed ({len(names)}): {', '.join(names)}"

    @server.tool(description="Profile Tracy zones matching a Python regex. Optionally reload a widget or gadget first with reload_kind='widget' or 'gadget' and reload_name.")
    def profile_zone_pattern(zone_pattern: str, duration: float = 5.0, reload_kind: str = "", reload_name: str = "") -> str:
        return _run_profile_with_retry(
            bar_backend,
            tracy_backend,
            notifier,
            zone_pattern,
            duration,
            reload_kind=reload_kind,
            reload_name=reload_name,
            diff=False,
        )

    @server.tool(description="Run two profiling passes for Tracy zones matching a Python regex and return the pass-to-pass delta. Optionally reload a widget or gadget before each pass.")
    def profile_zone_pattern_diff(zone_pattern: str, duration: float = 5.0, reload_kind: str = "", reload_name: str = "") -> str:
        return _run_profile_with_retry(
            bar_backend,
            tracy_backend,
            notifier,
            zone_pattern,
            duration,
            reload_kind=reload_kind,
            reload_name=reload_name,
            diff=True,
        )



def main() -> None:
    parser = argparse.ArgumentParser(
        description="BAR + Tracy Bridge MCP Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="MCP transport mode (default: stdio)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="SSE bind host")
    parser.add_argument("--port", type=int, default=0, help="SSE bind port (0 = auto)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("BAR + Tracy Bridge MCP Server starting")
    logger.info("BAR target:   %s:%d", BAR_HOST, BAR_PORT)
    logger.info("Tracy target: %s:%d", TRACY_HOST, TRACY_PORT)
    logger.info("Transport:    %s", args.transport)
    logger.info("=" * 60)

    server = create_bridge_server()
    notifier = ToolListNotifier()
    _enable_dynamic_tool_notifications(server, notifier)
    bar_backend = BarBackendSupervisor(server, notifier, BAR_HOST, BAR_PORT)
    tracy_backend = TracyBackendSupervisor(
        server,
        notifier,
        TRACY_HOST,
        TRACY_PORT,
        engine_address=TRACY_ENGINE_HOST,
        engine_port=TRACY_ENGINE_PORT,
        engine_alias=TRACY_ENGINE_ALIAS,
    )

    register_stable_bridge_tools(server, bar_backend, tracy_backend, notifier)

    if BRIDGE_STARTUP_BAR_PROBE:
        try:
            logger.info("Probing BAR before MCP startup so discovered tools appear in the first tools/list")
            bar_backend.ensure_ready(
                timeout=BRIDGE_STARTUP_BAR_TIMEOUT,
                connect_timeout=BRIDGE_STARTUP_BAR_TIMEOUT,
            )
        except Exception as exc:
            logger.info("BAR startup probe did not complete; background reconnect will continue: %s", exc)

    bar_backend.start()
    tracy_backend.start()

    try:
        if args.transport == "sse":
            port = args.port if args.port else 47381
            logger.info("Starting SSE server on %s:%d", args.host, port)
            server.run(host=args.host, port=port, transport="sse")
        else:
            logger.info("Starting stdio server")
            server.run(transport="stdio")
    except KeyboardInterrupt:
        logger.info("Shutting down (Ctrl+C)")
    finally:
        bar_backend.stop()
        tracy_backend.stop()


if __name__ == "__main__":
    main()
