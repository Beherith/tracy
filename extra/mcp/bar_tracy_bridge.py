# -*- coding: utf-8 -*-
"""
BAR + Tracy Bridge MCP Server

Sits between an AI client (Copilot, Claude, etc.) and local profiling backends:
  - BAR MCP (raw TCP JSON-RPC on 127.0.0.1:23452)
  - TracyServerBindings loaded in-process

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
    TRACY_ENGINE_HOST - Tracy engine hostname (default: 127.0.0.1)
    TRACY_ENGINE_PORT - Tracy engine port     (default: 8086)
    BRIDGE_LOG_LEVEL  - Log level         (default: INFO)
"""

from __future__ import annotations

import argparse
import asyncio
import builtins
import concurrent.futures
import inspect
import io
import json
import logging
import os
import random
import re
import socket
import struct
import sys
import threading
import time
import uuid
from contextlib import redirect_stdout
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
TRACY_ENGINE_PORT_RANGE = os.environ.get("TRACY_ENGINE_PORT_RANGE", "8086-8095")
TRACY_ENGINE_ALIAS = os.environ.get("TRACY_ENGINE_ALIAS", "live_engine")
_LLM_DIR = os.path.normpath(os.path.join(_HERE, "..", "..", "profiler", "src", "llm"))
_PROMPT_PATH = os.path.join(_LLM_DIR, "system.prompt.md")
_EVAL_GUIDE_PATH = os.path.join(_HERE, "eval_guide.md")
_PROTOCOL_HPP = os.path.normpath(os.path.join(_HERE, "..", "..", "public", "common", "TracyProtocol.hpp"))
_BROADCAST_PORT = 8086
_PROGRAM_NAME_SIZE = 64


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
    """Raised when the bridge cannot use the in-process Tracy backend."""


class Task:
    def __init__(self, task_id: str, code: str):
        self.id = task_id
        self.code = code
        self.status = "pending"
        self.result = None
        self.error = None
        self.start_time = time.time()
        self.end_time = None


class TracyInstance:
    def __init__(self, name: str, worker: object | None = None):
        self.name = name
        self.worker = worker


_tracy_instances: Dict[str, TracyInstance] = {}
_tracy_tasks: Dict[str, Task] = {}
_tracy_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
_tracy_bindings = None
_tracy_bindings_error: Optional[str] = None
_tracy_bindings_lock = threading.Lock()


def _read_text(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except Exception as exc:
        return f"(unavailable: {exc})"


def _read_bindings_protocol_version() -> Optional[int]:
    try:
        with open(_PROTOCOL_HPP, encoding="utf-8") as f:
            for line in f:
                match = re.search(r"constexpr\s+uint32_t\s+ProtocolVersion\s*=\s*(\d+)", line)
                if match:
                    return int(match.group(1))
    except Exception:
        pass
    return None


_OUR_PROTOCOL_VERSION = _read_bindings_protocol_version()


def _parse_broadcast(data: bytes) -> Optional[Dict[str, Any]]:
    if len(data) < 4:
        return None

    def _name(buf: bytes) -> str:
        return buf[:_PROGRAM_NAME_SIZE].split(b"\0", 1)[0].decode("utf-8", "replace")

    bv16 = struct.unpack_from("<H", data, 0)[0]
    if bv16 == 3 and len(data) >= 21:
        bv, lp, pv, pid, at = struct.unpack_from("<HHIQi", data, 0)
        return {"broadcast_version": bv, "listen_port": lp, "protocol_version": pv, "pid": pid, "active_seconds": at, "program": _name(data[20:])}
    if bv16 == 2 and len(data) >= 13:
        bv, lp, pv, at = struct.unpack_from("<HHIi", data, 0)
        return {"broadcast_version": bv, "listen_port": lp, "protocol_version": pv, "active_seconds": at, "program": _name(data[12:])}
    bv32 = struct.unpack_from("<I", data, 0)[0]
    if bv32 == 1 and len(data) >= 17:
        bv, pv, lp, at = struct.unpack_from("<IIII", data, 0)
        return {"broadcast_version": bv, "listen_port": lp, "protocol_version": pv, "active_seconds": at, "program": _name(data[16:])}
    if bv32 == 0 and len(data) >= 13:
        bv, pv, at = struct.unpack_from("<III", data, 0)
        return {"broadcast_version": bv, "listen_port": None, "protocol_version": pv, "active_seconds": at, "program": _name(data[12:])}
    return None


async def _listen_tracy_broadcasts(timeout_s: float = 1.5) -> List[Dict[str, Any]]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("", _BROADCAST_PORT))
    except OSError:
        sock.close()
        return []
    sock.setblocking(False)
    loop = asyncio.get_running_loop()
    seen: Dict[Optional[int], Dict[str, Any]] = {}
    deadline = loop.time() + timeout_s
    try:
        while loop.time() < deadline:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                data, _addr = await asyncio.wait_for(loop.sock_recvfrom(sock, 2048), timeout=remaining)
            except (asyncio.TimeoutError, BlockingIOError):
                break
            parsed = _parse_broadcast(data)
            if parsed:
                seen.setdefault(parsed.get("listen_port"), parsed)
    finally:
        sock.close()
    return list(seen.values())


def _load_tracy_bindings() -> Any:
    global _tracy_bindings, _tracy_bindings_error
    with _tracy_bindings_lock:
        if _tracy_bindings is not None:
            return _tracy_bindings
        errors: List[str] = []
        try:
            from tracy_client import TracyServerBindings as bindings

            _tracy_bindings = bindings
            _tracy_bindings_error = None
            return _tracy_bindings
        except BaseException as exc:
            errors.append(f"from tracy_client import TracyServerBindings: {exc}")

        build_path = os.path.normpath(os.path.join(_HERE, "../../build/python"))
        if build_path not in sys.path:
            sys.path.append(build_path)
        try:
            import TracyServerBindings as bindings

            _tracy_bindings = bindings
            _tracy_bindings_error = None
            return _tracy_bindings
        except BaseException as exc:
            errors.append(f"import TracyServerBindings from {build_path}: {exc}")

        _tracy_bindings_error = "; ".join(errors)
        logger.warning("Tracy Server bindings are unavailable: %s", _tracy_bindings_error)
        return None


def _run_coroutine_sync(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    box: Dict[str, Any] = {}

    def _runner() -> None:
        try:
            box["result"] = asyncio.run(coro)
        except BaseException as exc:
            box["error"] = exc

    thread = threading.Thread(target=_runner, daemon=True, name="tracy-local-async")
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    return box.get("result")


def _tracy_tool_result(value: Any, is_error: bool = False) -> Dict[str, Any]:
    if isinstance(value, str):
        text = value
    elif isinstance(value, (dict, list)):
        text = json.dumps(value)
    else:
        text = "" if value is None else str(value)
    result: Dict[str, Any] = {"content": [{"type": "text", "text": text}], "isError": is_error}
    if isinstance(value, (dict, list)):
        result["structuredContent"] = {"result": value}
    return result


async def _tracy_list_instances() -> List[Dict[str, Any]]:
    return [
        {"id": name, "live": True}
        for name, inst in _tracy_instances.items()
    ]


async def _tracy_discover_instances(port_range: str = "8086-8095") -> List[Dict[str, Any]]:
    start_port, end_port = map(int, port_range.split("-"))
    discovered: List[Dict[str, Any]] = []

    async def check_port(port: int) -> None:
        try:
            _, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), timeout=0.1)
            writer.close()
            await writer.wait_closed()
            discovered.append({"port": port, "address": "127.0.0.1"})
        except (OSError, asyncio.TimeoutError, ConnectionRefusedError):
            pass

    await asyncio.gather(*(check_port(port) for port in range(start_port, end_port + 1)))
    return discovered


async def _tracy_live_connect(address: str = "127.0.0.1", port: int = 8086, alias: Optional[str] = None) -> str:
    logger.info("live_connect called with address=%s, port=%s, alias=%s", address, port, alias)
    bindings = _load_tracy_bindings()
    if bindings is None:
        return f"Error: Tracy Server bindings not found. {_tracy_bindings_error or ''}".strip()

    broadcasts = await _listen_tracy_broadcasts(timeout_s=3.5)
    match = next((b for b in broadcasts if b.get("listen_port") == port), None)
    if match and _OUR_PROTOCOL_VERSION is not None and match["protocol_version"] != _OUR_PROTOCOL_VERSION:
        return (
            f"Protocol mismatch: target program '{match['program']}' announces Tracy protocol "
            f"v{match['protocol_version']} on {address}:{port}, but these server bindings are "
            f"built against v{_OUR_PROTOCOL_VERSION}. Rebuild the bindings or the target "
            f"against a matching Tracy version."
        )

    try:
        worker = bindings.Worker(address, port)
    except BaseException as exc:
        logger.error("Failed to construct Tracy worker for %s:%s: %s", address, port, exc)
        return f"Failed to connect: {exc}"

    deadline_s = 2.0
    step_s = 0.1
    elapsed = 0.0
    while elapsed < deadline_s:
        try:
            if worker.is_connected():
                break
        except BaseException as exc:
            try:
                worker.shutdown()
            except BaseException:
                pass
            return f"Failed to connect: worker connection check failed: {exc}"
        await asyncio.sleep(step_s)
        elapsed += step_s

    try:
        connected = worker.is_connected()
    except BaseException:
        connected = False
    if not connected:
        try:
            worker.shutdown()
        except BaseException:
            pass
        if broadcasts and not match:
            seen = ", ".join(
                f"'{b['program']}' on port {b.get('listen_port')} (protocol v{b['protocol_version']})"
                for b in broadcasts
            )
            hint = f" Detected other Tracy broadcasts: {seen}."
        elif not broadcasts:
            hint = (
                " No Tracy broadcasts were received on port 8086 in 3.5s. "
                "The target may use TRACY_ON_DEMAND, a non-default broadcast port, or is not running."
            )
        else:
            hint = ""
        return (
            f"Reached {address}:{port} but the Tracy handshake did not complete within "
            f"{deadline_s:.1f}s.{hint} Common causes: version mismatch, TRACY_ON_DEMAND "
            f"waiting for a profiler request, or another client already attached."
        )

    name = alias or f"live_{address}_{port}"
    _tracy_instances[name] = TracyInstance(name, worker)
    return (
        f"Connected to live instance as '{name}'. Before your first eval, read resources "
        f"tracy://prompt and tracy://eval-guide."
    )


async def _tracy_disconnect_instance(instance_id: str) -> str:
    inst = _tracy_instances.pop(instance_id, None)
    if inst is None:
        return f"Instance '{instance_id}' not found."
    worker = inst.worker
    if worker is not None:
        try:
            worker.shutdown()
        except BaseException:
            pass
    return f"Instance '{instance_id}' disconnected."


def _execute_tracy_eval_sync(code: str, ctx: object) -> str:
    bindings = _load_tracy_bindings()
    global_vars = {
        "__builtins__": builtins,
        "ctx": ctx,
        "tracy": bindings,
        "instances": {name: inst.worker for name, inst in _tracy_instances.items()},
    }
    buf = io.StringIO()
    with redirect_stdout(buf):
        try:
            result = eval(compile(code, "<eval>", "eval"), global_vars)
        except SyntaxError:
            exec(compile(code, "<exec>", "exec"), global_vars)
            result = None
    output = buf.getvalue()
    if result is None:
        return output or ""
    return str(result)


async def _execute_tracy_eval(code: str, ctx: object) -> str:
    return await asyncio.get_running_loop().run_in_executor(_tracy_executor, _execute_tracy_eval_sync, code, ctx)


def _run_tracy_task_sync(task: Task, worker: object) -> None:
    task.status = "running"
    try:
        task.result = _execute_tracy_eval_sync(task.code, worker)
        task.status = "completed"
    except BaseException as exc:
        task.error = str(exc)
        task.status = "failed"
    finally:
        task.end_time = time.time()


async def _tracy_eval(code: str, instance_id: str, async_mode: bool = False) -> Any:
    if instance_id not in _tracy_instances:
        return f"Error: Instance '{instance_id}' not found. Use list_instances to find valid IDs."
    instance = _tracy_instances[instance_id]
    if not instance.worker:
        return f"Error: Instance '{instance_id}' has no worker."
    if not async_mode:
        return await _execute_tracy_eval(code, instance.worker)

    task_id = str(uuid.uuid4())
    task = Task(task_id, code)
    _tracy_tasks[task_id] = task
    asyncio.get_running_loop().run_in_executor(_tracy_executor, _run_tracy_task_sync, task, instance.worker)
    return {"task_id": task_id, "status": "running"}


async def _tracy_task(action: str, task_id: Optional[str] = None) -> Any:
    if action == "list":
        return [{"id": task.id, "status": task.status, "elapsed": time.time() - task.start_time} for task in _tracy_tasks.values()]
    if not task_id or task_id not in _tracy_tasks:
        return "Error: Task ID not found."
    task = _tracy_tasks[task_id]
    if action == "poll":
        result: Dict[str, Any] = {"id": task.id, "status": task.status}
        if task.status == "completed":
            result["result"] = task.result
        elif task.status == "failed":
            result["error"] = task.error
        return result
    if action == "cancel":
        if task.status == "running":
            task.status = "cancelled"
            return f"Task {task_id} marked as cancelled."
        return f"Task {task_id} is not running."
    return "Error: Unknown action."


TRACY_LOCAL_TOOL_SCHEMAS: List[Dict[str, Any]] = [
    {"name": "list_instances", "description": "List live Tracy engine connections.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "discover_instances", "description": "Scan local ports for running Tracy-instrumented applications.", "inputSchema": {"type": "object", "properties": {"port_range": {"type": "string", "default": "8086-8095"}}}},
    {"name": "live_connect", "description": "Connect to a live Tracy-instrumented application.", "inputSchema": {"type": "object", "properties": {"address": {"type": "string", "default": "127.0.0.1"}, "port": {"type": "integer", "default": 8086}, "alias": {"type": "string"}}, "required": []}},
    {"name": "disconnect_instance", "description": "Disconnect a live Tracy engine instance.", "inputSchema": {"type": "object", "properties": {"instance_id": {"type": "string"}}, "required": ["instance_id"]}},
    {"name": "eval", "description": "Execute Python code against a Tracy Worker bound as ctx.", "inputSchema": {"type": "object", "properties": {"code": {"type": "string"}, "instance_id": {"type": "string"}, "async_mode": {"type": "boolean", "default": False}}, "required": ["code", "instance_id"]}},
    {"name": "task", "description": "Manage background Tracy eval tasks.", "inputSchema": {"type": "object", "properties": {"action": {"type": "string"}, "task_id": {"type": "string"}}, "required": ["action"]}},
]


class TracyLocalClient:
    """In-process Tracy tool client folded from tracy_mcp.py.

    The name is retained for the bridge code/tests that only need a client with
    connected, discover_tools, call_tool, and disconnect methods.
    """

    def __init__(self, host: str = TRACY_HOST, port: int = TRACY_PORT, timeout: float = 30.0):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def bindings_available(self) -> bool:
        return _load_tracy_bindings() is not None

    @property
    def bindings_error(self) -> Optional[str]:
        if _tracy_bindings is None and _tracy_bindings_error is None:
            _load_tracy_bindings()
        return _tracy_bindings_error

    def connect(self) -> None:
        self._connected = True
        bindings = _load_tracy_bindings()
        if bindings is None:
            logger.warning("Tracy local backend is up, but bindings are unavailable: %s", _tracy_bindings_error)
        else:
            logger.info("Tracy local backend ready with TracyServerBindings loaded")

    def disconnect(self) -> None:
        self._connected = False

    def discover_tools(self, timeout: float = 15.0) -> List[Dict[str, Any]]:
        return [dict(tool) for tool in TRACY_LOCAL_TOOL_SCHEMAS]

    def call_tool(self, tool_name: str, arguments: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None) -> Any:
        if not self.connected:
            raise TracyConnectionError("Tracy local backend is not connected.")
        args = arguments or {}
        try:
            if tool_name == "list_instances":
                value = _run_coroutine_sync(_tracy_list_instances())
            elif tool_name == "discover_instances":
                value = _run_coroutine_sync(_tracy_discover_instances(**args))
            elif tool_name == "live_connect":
                value = _run_coroutine_sync(_tracy_live_connect(**args))
            elif tool_name == "disconnect_instance":
                value = _run_coroutine_sync(_tracy_disconnect_instance(**args))
            elif tool_name == "eval":
                value = _run_coroutine_sync(_tracy_eval(**args))
            elif tool_name == "task":
                value = _run_coroutine_sync(_tracy_task(**args))
            else:
                raise TracyConnectionError(f"Unknown Tracy tool '{tool_name}'.")
        except TypeError as exc:
            raise TracyConnectionError(f"Invalid arguments for Tracy tool '{tool_name}': {exc}") from exc
        return _tracy_tool_result(value, is_error=isinstance(value, str) and value.startswith("Error:"))


class TracyToolRegistry:
    """Discovers in-process Tracy tools for internal bridge use.

    The bridge deliberately does not expose raw Tracy tools to the client.
    Profiling is the public surface; discovered Tracy tools stay behind the
    supervisor so low-level Tracy utilities do not clutter MCP tool lists.
    """

    def __init__(self, client: "TracyLocalClient"):
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
        """Query the local Tracy client for the list of available tools."""
        logger.info("Discovering local Tracy tools ...")
        tools = self._client.discover_tools(timeout)
        self._tools = tools
        self._tool_map = {t["name"]: t for t in self._tools}
        logger.info(
            "Cached %d Tracy tools: %s",
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
            Raw result dict in MCP tool-result shape.
        """
        logger.debug("[BRIDGE:TRACY_REG] call_tool name='%s' arguments=%s", name, arguments)
        if name not in self._tool_map:
            available = ", ".join(self._tool_map.keys())
            raise TracyConnectionError(
                f"Unknown Tracy tool '{name}'. Available: {available}. "
                f"Call discover() to refresh the tool list."
            )

        return self._client.call_tool(name, arguments, timeout)


TracyHttpClient = TracyLocalClient


# ---------------------------------------------------------------------------
# Phase 3 - Profile Tools
# ---------------------------------------------------------------------------


class ProfileCollector:
    """Orchestrates optional reload, profiling wait, and Tracy zone collection."""

    def __init__(
        self,
        bar_registry: BarToolRegistry,
        tracy_client: TracyLocalClient,
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

Tracy profiling should be done by adding searchable zones in the Lua code, e.g. tracy.ZoneBeginN("My Widget:Update") / tracy.ZoneEnd(), then calling profile_zone_pattern with a regex such as "^My Widget:".

Example:

'''lua 

function widget:GetInfo() -- similarly for gadget:GetInfo()
	return {
		name = "My Widget", -- use this name in zone name pattern
		desc = "Description",
        -- ...other widget info...
	}
end


function foo(bar)
    tracy.ZoneBeginN("My Widget:foo") -- start a zone with a custom name (appears in Tracy UI)
    
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
- Some functions are already pre-instrumented, such as `widget:GameFrame()`, with the zone naming: "W:GameFrame:My Widget" for widget code and "G:GameFrame:My Gadget" for gadget code.
- You only need to do short replace_string_in_file tool calls to add zones, you dont need to repeat the entire function body. Use the existing code and just add tracy.ZoneBeginN / ZoneEnd calls around the parts you want to profile in separate replace_string_in_file tool calls. 
- IMPORTANT: Do not use any emojis, symbols, or decorative characters in your responses.
- FORMAT: Respond exclusively using plain text and standard punctuation.

""")
    if hasattr(server, "resource"):
        @server.resource("tracy://prompt")
        def tracy_prompt_resource() -> str:
            return _read_text(_PROMPT_PATH)

        @server.resource("tracy://eval-guide")
        def tracy_eval_guide_resource() -> str:
            return _read_text(_EVAL_GUIDE_PATH)

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
    """Keeps the in-process Tracy tools and live engine instance reconnectable."""

    def __init__(
        self,
        server: Any,
        notifier: ToolListNotifier,
        host: str = TRACY_HOST,
        port: int = TRACY_PORT,
        engine_address: str = "127.0.0.1",
        engine_port: int = 8086,
        engine_port_range: str = TRACY_ENGINE_PORT_RANGE,
        engine_alias: str = "live_engine",
        client_factory: Optional[Callable[[str, int], TracyLocalClient]] = None,
    ) -> None:
        self._server = server
        self._notifier = notifier
        self._host = host
        self._port = port
        self._engine_address = engine_address
        self._engine_port = engine_port
        self._engine_port_range = engine_port_range
        self._engine_alias = engine_alias
        self._client_factory = client_factory or (lambda host, port: TracyLocalClient(host, port))
        self._lock = threading.RLock()
        self._connect_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._installed_tools: Set[str] = set()
        self._profile_tool_defs: Dict[str, tuple[str, Callable[..., Any]]] = {}
        self._backoff = 1.0
        self.client: Optional[TracyLocalClient] = None
        self.registry: Optional[TracyToolRegistry] = None
        self.mcp_state = "offline"
        self.engine_state = "offline"
        self.generation = 0
        self.engine_generation = 0
        self.instance_id: Optional[str] = None
        self.last_discovered: List[Dict[str, Any]] = []
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
                "target": "in-process",
                "engine_target": f"{self._engine_address}:{self._engine_port}",
                "engine_scan_range": self._engine_port_range,
                "engine_discovered": list(self.last_discovered),
                "profile_tool_names": sorted(self._installed_tools),
                "bindings_available": getattr(self.client, "bindings_available", None) if self.client else _tracy_bindings is not None,
                "bindings_error": getattr(self.client, "bindings_error", None) if self.client else _tracy_bindings_error,
            }

    def set_profile_tools(self, tools: Dict[str, tuple[str, Callable[..., Any]]]) -> None:
        with self._lock:
            self._profile_tool_defs = dict(tools)
        self._install_dynamic_tools()

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
            client = self._client_factory(self._host, self._port)
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
            logger.info("Tracy local backend ready (generation %d)", self.generation)
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
            raise TracyConnectionError("Tracy local client is unavailable.")
        with self._lock:
            self.engine_state = "connecting"
        instance_id = self._scan_and_connect_engine(client)
        if not instance_id:
            with self._lock:
                self.engine_state = "offline"
                self.last_error = "Tracy local backend could not connect to the live engine."
            self._install_dynamic_tools()
            raise TracyConnectionError(f"Tracy local backend could not connect to the engine at {self._engine_address}:{self._engine_port}.")
        with self._lock:
            self.instance_id = instance_id
            self.engine_state = "ready"
            self.engine_generation += 1
            self.last_error = None
        self._install_dynamic_tools()
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
        logger.warning("Tracy local backend marked offline: %s", error)
        with self._lock:
            self.mcp_state = "offline"
            self.engine_state = "offline"
            self.instance_id = None
            self.last_error = error
            self.generation += 1
        self._disconnect_client()
        self._install_dynamic_tools()

    def mark_engine_offline(self, error: str) -> None:
        logger.warning("Tracy engine backend marked offline: %s", error)
        with self._lock:
            self.engine_state = "offline"
            self.instance_id = None
            self.last_error = error
            self.engine_generation += 1
        self._install_dynamic_tools()

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

    def _scan_and_connect_engine(self, client: TracyLocalClient) -> Optional[str]:
        if not client.connected:
            logger.warning("Tracy local backend is not connected; cannot auto-connect engine")
            return None

        targets = self._engine_scan_targets(client)
        logger.info(
            "Scanning %d Tracy engine target(s): %s",
            len(targets),
            ", ".join(f"{host}:{port}" for host, port in targets),
        )
        failures: List[str] = []
        for address, port in targets:
            instance_id = self._try_connect_engine(client, address, port)
            if instance_id:
                with self._lock:
                    self._engine_address = address
                    self._engine_port = port
                return instance_id
            failures.append(f"{address}:{port}")
        logger.warning("No Tracy engine connected after scanning: %s", ", ".join(failures))
        return None

    def _engine_scan_targets(self, client: TracyLocalClient) -> List[tuple[str, int]]:
        targets: List[tuple[str, int]] = [(self._engine_address, self._engine_port)]
        discovered: List[Dict[str, Any]] = []
        try:
            result = client.call_tool("discover_instances", {"port_range": self._engine_port_range}, timeout=5.0)
            data = _mcp_result_to_data(result)
            if isinstance(data, list):
                discovered = [item for item in data if isinstance(item, dict)]
        except Exception as exc:
            logger.debug("Tracy engine port scan failed for range %s: %s", self._engine_port_range, exc)

        with self._lock:
            self.last_discovered = list(discovered)

        for item in discovered:
            try:
                port = int(item.get("port"))
            except (TypeError, ValueError):
                continue
            address = str(item.get("address") or self._engine_address or "127.0.0.1")
            targets.append((address, port))

        unique: List[tuple[str, int]] = []
        seen: Set[tuple[str, int]] = set()
        for target in targets:
            if target not in seen:
                seen.add(target)
                unique.append(target)
        return unique

    def _try_connect_engine(self, client: TracyLocalClient, address: str, port: int) -> Optional[str]:
        logger.info("Auto-connecting Tracy backend to engine at %s:%d ...", address, port)
        try:
            result = client.call_tool(
                "live_connect",
                {"address": address, "port": port, "alias": self._engine_alias},
                timeout=15.0,
            )
            text_result = _mcp_result_to_text(result)
            if text_result.startswith(("Error", "Failed", "Protocol mismatch", "Reached ")):
                logger.warning("Tracy live_connect failed: %s", text_result)
                return None
            match = re.search(r"as '([^']+)'", text_result)
            instance_id = match.group(1) if match else self._engine_alias
            logger.info("Tracy connected to engine: %s", text_result[:100])
            return instance_id
        except Exception as exc:
            logger.warning("Tracy live_connect failed for %s:%d alias=%s: %s", address, port, self._engine_alias, exc)
            return None

    def _install_dynamic_tools(self) -> None:
        with self._lock:
            desired = set(self._profile_tool_defs.keys()) if self.engine_state == "ready" else set()
            installed = set(self._installed_tools)
            tool_defs = dict(self._profile_tool_defs)

        changed = False
        for old_name in installed - desired:
            changed = _remove_tool(self._server, old_name) or changed
        for tool_name in desired:
            desc, func = tool_defs[tool_name]
            _install_or_replace_tool(self._server, tool_name, desc, func)
            changed = True

        with self._lock:
            self._installed_tools = desired

        if changed or installed != desired:
            self._notifier.notify("tracy", sorted(desired))

    def _monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            wait = 5.0
            try:
                if self.mcp_state != "ready":
                    self.ensure_mcp_ready(timeout=5.0)
                elif self.client and not self.client.connected:
                    self.mark_mcp_offline("Tracy local client disconnected")
                elif self.engine_state != "ready":
                    self.ensure_engine_ready()
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
            "mode": "in-process",
            "generation": tracy["generation"],
            "tools": tracy["tools"],
            "tool_names": tracy["tool_names"],
            "last_error": tracy["last_error"],
            "last_ready_at": tracy["last_ready_at"],
            "target": tracy["target"],
            "bindings_available": tracy.get("bindings_available"),
            "bindings_error": tracy.get("bindings_error"),
        },
        "tracy_engine": {
            "state": tracy["engine_state"],
            "generation": tracy["engine_generation"],
            "instance_id": tracy["instance_id"],
            "target": tracy["engine_target"],
            "scan_range": tracy.get("engine_scan_range"),
            "discovered": tracy.get("engine_discovered", []),
            "profile_tool_names": tracy.get("profile_tool_names", []),
        },
        "profile_tools_available": tracy["mcp_state"] == "ready" and tracy["engine_state"] == "ready",
        "profile_reload_available": bar["state"] == "ready" and tracy["mcp_state"] == "ready" and tracy["engine_state"] == "ready",
        "tool_notification_generation": notifier.generation,
        "tool_notifications": notifier.status(),
        "config": {
            "bar_target": f"{BAR_HOST}:{BAR_PORT}",
            "tracy_mcp_target": "in-process",
            "tracy_engine_target": f"{TRACY_ENGINE_HOST}:{TRACY_ENGINE_PORT}",
            "tracy_engine_port_range": TRACY_ENGINE_PORT_RANGE,
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
    @server.tool(description="Return BAR, local Tracy backend, Tracy engine, and dynamic tool discovery status.")
    def bridge_status() -> str:
        return json.dumps(_bridge_status_payload(bar_backend, tracy_backend, notifier), indent=2)

    @server.tool(description="List currently installed dynamic BAR convenience tools.")
    def bridge_dynamic_tools() -> str:
        status = _bridge_status_payload(bar_backend, tracy_backend, notifier)
        dynamic = {
            "bar": status["bar"]["tool_names"],
            "tracy": status["tracy_engine"].get("profile_tool_names", []),
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

    tracy_backend.set_profile_tools(
        {
            "profile_zone_pattern": (
                "Profile Tracy zones matching a Python regex. Optionally reload a widget or gadget first with reload_kind='widget' or 'gadget' and reload_name.",
                profile_zone_pattern,
            ),
            "profile_zone_pattern_diff": (
                "Run two profiling passes for Tracy zones matching a Python regex and return the pass-to-pass delta. Optionally reload a widget or gadget before each pass.",
                profile_zone_pattern_diff,
            ),
        }
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
    logger.info("Tracy backend: in-process TracyServerBindings")
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
        engine_port_range=TRACY_ENGINE_PORT_RANGE,
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
