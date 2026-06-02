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
    BAR_MCP_HOST      — BAR MCP hostname  (default: 127.0.0.1)
    BAR_MCP_PORT      — BAR MCP port      (default: 23452)
    TRACY_MCP_HOST    — Tracy MCP hostname (default: 127.0.0.1)
    TRACY_MCP_PORT    — Tracy MCP port    (default: 47380)
    BRIDGE_LOG_LEVEL  — Log level         (default: INFO)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_LOG_LEVEL = os.environ.get("BRIDGE_LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("bar_tracy_bridge")
logger.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))

_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.setFormatter(
    logging.Formatter("[%(asctime)s] [BRIDGE] %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
)
logger.addHandler(_stderr_handler)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BAR_HOST = os.environ.get("BAR_MCP_HOST", "127.0.0.1")
BAR_PORT = int(os.environ.get("BAR_MCP_PORT", "23452"))

TRACY_HOST = os.environ.get("TRACY_MCP_HOST", "127.0.0.1")
TRACY_PORT = int(os.environ.get("TRACY_MCP_PORT", "47380"))

_HERE = os.path.dirname(os.path.abspath(__file__))
_TRACY_MCP_SCRIPT = os.path.join(_HERE, "tracy_mcp.py")
_TRACY_MCP_PID_FILE = os.path.join(_HERE, "tracy_mcp.pid")

# ---------------------------------------------------------------------------
# Phase 1.1 — BarTcpClient
# ---------------------------------------------------------------------------


class BarConnectionError(Exception):
    """Raised when the bridge cannot connect to BAR MCP."""


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
    ):
        self._host = host
        self._port = port
        self._reconnect_max = reconnect_max
        self._reconnect_base = reconnect_base

        self._sock: Optional[socket.socket] = None
        self._buffer = ""
        self._lock = threading.Lock()
        self._request_id = 0

        # Response demultiplexing: request_id -> (Event, result_or_error)
        self._pending: Dict[int, threading.Event] = {}
        self._pending_results: Dict[int, Any] = {}
        self._reader_running = False
        self._reader_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._sock is not None

    def connect(self) -> None:
        """Establish a TCP connection to BAR MCP.

        Raises BarConnectionError if the connection cannot be established.
        """
        addr = f"{self._host}:{self._port}"
        logger.info("Connecting to BAR MCP on %s …", addr)

        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(5.0)
            self._sock.connect((self._host, self._port))
            self._sock.settimeout(1.0)  # non-blocking reads for reader loop
            self._buffer = ""
            logger.info("Connected to BAR MCP on %s", addr)
        except OSError as exc:
            self._cleanup_sock()
            raise BarConnectionError(
                f"Cannot connect to BAR MCP on {addr} — "
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
    def _parse_jsonrpc_line(line: str) -> Optional[Dict[str, Any]]:
        """Parse a single newline-delimited JSON-RPC line. Returns None on error."""
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            logger.error("Invalid JSON from BAR MCP: %s", line[:200])
            return None
        if not isinstance(msg, dict):
            return None
        return msg

    def _reader_loop(self) -> None:
        """Background thread: read from socket, parse JSON-RPC lines, demultiplex by id.

        Invariant: complete lines are always parsed from _buffer *before* calling
        recv().  If a previous recv() returned two responses in one chunk, the
        second sits in _buffer and is consumed immediately on the next loop
        iteration — we never block on the network when data is already available.
        """
        logger.debug("BAR TCP reader thread started")
        while self._reader_running:
            try:
                # --- Phase 1: parse any complete lines already in the buffer ---
                parsed_line = False
                while self._reader_running:
                    nl_pos = self._buffer.find("\n")
                    if nl_pos < 0:
                        break
                    line = self._buffer[:nl_pos]
                    self._buffer = self._buffer[nl_pos + 1:]
                    parsed_line = True

                    if not line.strip():
                        continue

                    msg = self._parse_jsonrpc_line(line)
                    if msg is None:
                        continue

                    resp_id = msg.get("id")
                    if resp_id is not None:
                        ev = self._pending.pop(int(resp_id), None)
                        if ev:
                            self._pending_results[int(resp_id)] = msg
                            ev.set()
                            logger.debug("BAR TCP ◄ response id=%s delivered", resp_id)
                        else:
                            logger.warning(
                                "BAR TCP ◄ unsolicited response id=%s (no pending request)",
                                resp_id,
                            )
                    else:
                        logger.debug("BAR TCP ◄ notification or stray: %s", line[:100])

                # --- Phase 2: recv only when no complete line was available ---
                if self._sock and not parsed_line:
                    try:
                        chunk = self._sock.recv(4096)
                    except socket.timeout:
                        continue
                    except OSError:
                        break

                    if not chunk:
                        break

                    self._buffer += chunk.decode("utf-8", errors="replace")
                    # Loop back to Phase 1 to parse the newly appended data

                # --- Phase 3: detect closed socket (only when idle) ---
                if self._sock and self._buffer == "" and not parsed_line:
                    # Socket exists but we got nothing and buffer is empty —
                    # check if it was closed by attempting a zero-byte recv.
                    pass  # timeout in Phase 2 will handle this on next iteration

                if not self._sock:
                    break

            except Exception as exc:
                logger.warning("BAR TCP reader thread error: %s", exc)
                break

        logger.debug("BAR TCP reader thread stopped")
        # Notify any pending waiters that the connection is gone
        for ev in self._pending.values():
            self._pending_results[-1] = BarConnectionError(
                "BAR MCP connection lost while waiting for response."
            )
            ev.set()
        self._pending.clear()
        self._pending_results.clear()

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
        ev = threading.Event()
        with self._lock:
            self._pending[request_id] = ev

        if not ev.wait(timeout=timeout):
            with self._lock:
                self._pending.pop(request_id, None)
            raise BarConnectionError(
                f"Timeout waiting for BAR MCP response id={request_id} after {timeout}s. "
                f"Is dbg_bar_mcp.lua loaded? Check game console for '[BARMCP]' messages."
            )

        result = self._pending_results.pop(request_id, None)
        if isinstance(result, BarConnectionError):
            raise result
        if result is None:
            raise BarConnectionError(
                f"No response received for BAR MCP request id={request_id}."
            )
        return result

    def reconnect(self) -> None:
        """Reconnect with exponential backoff.

        Raises BarConnectionError after exhausting all attempts.
        """
        self._stop_reader()
        self._cleanup_sock()
        # Clear pending requests so old waiters don't block forever
        for ev in self._pending.values():
            self._pending_results[-1] = BarConnectionError(
                "Reconnecting — previous request cancelled."
            )
            ev.set()
        self._pending.clear()
        self._pending_results.clear()

        addr = f"{self._host}:{self._port}"
        last_error: Optional[Exception] = None

        for attempt in range(1, self._reconnect_max + 1):
            wait = self._reconnect_base * (2 ** (attempt - 1))
            logger.warning(
                "Reconnect attempt %d/%d to BAR MCP on %s (waiting %.1fs) …",
                attempt, self._reconnect_max, addr, wait,
            )
            time.sleep(wait)

            try:
                self.connect()
                return  # success
            except BarConnectionError as exc:
                last_error = exc
                logger.warning("Reconnect attempt %d failed: %s", attempt, exc)

        raise BarConnectionError(
            f"Failed to reconnect to BAR MCP on {addr} after {self._reconnect_max} attempts. "
            f"Last error: {last_error}"
        ) from last_error

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
                "Not connected to BAR MCP — call connect() first. "
                "Is the game running with dev mode enabled?"
            )

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
        logger.debug("BAR TCP ► %s", payload.rstrip())

        try:
            if self._sock:
                self._sock.sendall(payload.encode("utf-8"))
        except OSError as exc:
            self._cleanup_sock()
            raise BarConnectionError(
                f"Lost connection to BAR MCP while sending — {exc}"
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
        logger.debug("BAR TCP ► (notification) %s", payload.rstrip())

        with self._lock:
            try:
                if self._sock:
                    self._sock.sendall(payload.encode("utf-8"))
            except OSError as exc:
                self._cleanup_sock()
                raise BarConnectionError(
                    f"Lost connection to BAR MCP while sending notification — {exc}"
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
        req_id = self.send_jsonrpc(method, params)
        response = self.receive_response(req_id, timeout)
        logger.debug("BAR TCP ◄ response id=%s", response.get("id"))
        return response

    def call_tool(self, tool_name: str, arguments: Optional[Dict[str, Any]] = None, timeout: float = 30.0) -> Dict[str, Any]:
        """Call a BAR MCP tool via `tools/call` and return the result.

        Returns the raw result dict from BAR (contains 'content' and 'isError').
        Raises BarConnectionError if the tool returned isError=true.
        """
        params: Dict[str, Any] = {"name": tool_name}
        if arguments:
            params["arguments"] = arguments

        response = self.call_method("tools/call", params, timeout)

        if "error" in response:
            raise BarConnectionError(
                f"BAR MCP JSON-RPC error for '{tool_name}': "
                f"{response['error'].get('message', 'unknown')}"
            )

        result = response.get("result", {})
        if result.get("isError"):
            text = self._extract_text(result)
            raise BarConnectionError(
                f"BAR MCP tool '{tool_name}' returned error: {text} — "
                f"check game console for details."
            )

        return result

    def ping(self, timeout: float = 5.0) -> bool:
        """Send a heartbeat ping to BAR MCP and verify response."""
        try:
            # We call the 'ping' tool we just added to dbg_bar_mcp.lua
            result = self.call_tool("ping", timeout=timeout)
            return True
        except BarConnectionError as exc:
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
# Phase 1.2 — BAR Tool Discovery & Forwarding
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

    def discover(self, timeout: float = 15.0) -> List[Dict[str, Any]]:
        """Query BAR MCP for the list of available tools.

        Sends `tools/list` and caches the result.
        """
        logger.info("Discovering BAR tools …")
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

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None, timeout: float = 30.0) -> str:
        """Call a BAR tool by name and return the plain text result.

        Args:
            name: Tool name (e.g., "game_info", "widget_reload")
            arguments: Dict of arguments matching the tool's inputSchema
            timeout: Max seconds to wait for response

        Returns:
            Plain text result string from the tool.
        """
        if name not in self._tool_map:
            available = ", ".join(self._tool_map.keys())
            raise BarConnectionError(
                f"Unknown BAR tool '{name}'. Available: {available}. "
                f"Call discover() to refresh the tool list."
            )

        raw_result = self._client.call_tool(name, arguments, timeout)
        return BarTcpClient._extract_text(raw_result)


# ---------------------------------------------------------------------------
# Phase 2 — Tracy Integration
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
        logger.info("Connecting to Tracy MCP on %s …", addr)

        try:
            import httpx
        except ImportError:
            raise TracyConnectionError(
                "httpx is required for Tracy MCP communication. "
                "Install with: pip install httpx"
            )

        # Import httpx once and cache it
        self._httpx = httpx

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
                f"Cannot connect to Tracy MCP on {addr} — "
                f"is Tracy MCP running? Check that tracy_mcp.py exists and "
                f"TracyServerBindings are built. Detail: {exc}"
            ) from exc

        # Start background SSE reader thread (it will detect the endpoint event)
        self._start_sse_reader()

        # Wait for the endpoint event from the reader thread
        if not self._endpoint_event.wait(timeout=self._timeout):
            self._stop_sse_reader()
            self._cleanup_sse()
            raise TracyConnectionError(
                f"Tracy MCP SSE handshake failed — no 'endpoint' event received within {self._timeout}s. "
                f"Is Tracy MCP running on {addr}?"
            )

        if not self._message_url:
            self._stop_sse_reader()
            self._cleanup_sse()
            raise TracyConnectionError(
                f"Tracy MCP SSE handshake failed — endpoint event had no URL. "
                f"Is Tracy MCP running on {addr}?"
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
        """
        event_type = "message"  # default event type
        data = ""

        for line in text.split("\n"):
            if line.startswith("event: "):
                event_type = line[7:].strip()
            elif line.startswith("data: "):
                data = line[6:].strip()

        return event_type, data

    def disconnect(self) -> None:
        """Close the SSE session and stop the reader thread."""
        if self._connected:
            logger.info("Disconnecting from Tracy MCP")
        self._stop_sse_reader()
        self._cleanup_sse()
        self._connected = False
        self._message_url = None

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
                        # First endpoint event — store URL and signal connect()
                        if not self._message_url:
                            self._message_url = data.strip()
                            logger.debug("Tracy SSE ► endpoint event: %s", self._message_url)
                            self._endpoint_event.set()
                        else:
                            logger.debug("Tracy SSE ► duplicate endpoint: %s", data)
                    elif event_type == "result":
                        # This is a JSON-RPC response
                        self._handle_sse_response(data)
                    elif event_type == "error":
                        logger.warning("Tracy SSE ► error event: %s", data[:200])
                    else:
                        logger.debug("Tracy SSE ► event [%s]: %s", event_type, data[:100])

        except Exception as exc:
            logger.warning("Tracy SSE reader thread error: %s", exc)
        finally:
            # Notify any pending waiters that the connection is gone
            for ev in self._pending.values():
                self._pending_results[-1] = TracyConnectionError(
                    "Tracy MCP SSE connection lost while waiting for response."
                )
                ev.set()
            self._pending.clear()
            self._pending_results.clear()
            logger.debug("Tracy SSE reader thread stopped")

    def _handle_sse_response(self, data: str) -> None:
        """Parse an SSE 'result' event and deliver to the waiting request."""
        try:
            msg = json.loads(data)
        except json.JSONDecodeError:
            logger.error("Tracy SSE ► invalid JSON: %s", data[:200])
            return

        if not isinstance(msg, dict):
            logger.warning("Tracy SSE ► non-dict response: %s", data[:100])
            return

        resp_id = msg.get("id")
        if resp_id is not None:
            with self._lock:
                ev = self._pending.pop(int(resp_id), None)
                if ev:
                    self._pending_results[int(resp_id)] = msg
                    ev.set()
                    logger.debug("Tracy SSE ◄ response id=%s delivered", resp_id)
                else:
                    logger.warning(
                        "Tracy SSE ◄ unsolicited response id=%s (no pending request)",
                        resp_id,
                    )
        else:
            logger.debug("Tracy SSE ◄ notification or stray: %s", data[:100])

    # ------------------------------------------------------------------
    # JSON-RPC messaging
    # ------------------------------------------------------------------

    def _next_id(self) -> int:
        """Generate the next unique request ID."""
        self._request_id += 1
        return self._request_id

    def _wait_for_response(self, request_id: int, timeout: float) -> Dict[str, Any]:
        """Wait for the SSE response matching a specific request ID."""
        ev = threading.Event()
        with self._lock:
            self._pending[request_id] = ev

        if not ev.wait(timeout=timeout):
            with self._lock:
                self._pending.pop(request_id, None)
            raise TracyConnectionError(
                f"Timeout waiting for Tracy MCP response id={request_id} after {timeout}s."
            )

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
                "Not connected to Tracy MCP — call connect() first. "
                "Is Tracy MCP running?"
            )

        req_id = self._next_id()
        msg: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
            "id": req_id,
        }
        if params is not None:
            msg["params"] = params

        req_timeout = timeout or self._timeout

        logger.debug("Tracy SSE ► POST id=%s method=%s", req_id, method)

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
                # Don't raise yet — the response might still arrive via SSE
                # Fall through to wait_for_response

        except Exception as exc:
            logger.warning("Tracy SSE POST error: %s", exc)
            # Fall through to wait_for_response — might still arrive

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

        logger.debug("Tracy SSE ► (notification) %s", json.dumps(msg)[:200])

        try:
            self._httpx.post(
                self._message_url,
                json=msg,
                timeout=self._timeout,
            )
        except Exception as exc:
            logger.warning("Tracy SSE notification POST error: %s", exc)

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
                response = httpx.get(
                    url,
                    timeout=2.0,
                    headers={"Accept": "text/event-stream"},
                )
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
            logger.info("Tracy MCP already running (PID file: %s)", self._pid_file)
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

        logger.info("Starting Tracy MCP …")
        try:
            python = sys.executable or "python3"
            self._process = subprocess.Popen(
                [python, self._script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
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
            "Auto-connecting Tracy MCP to engine at %s:%d …", address, port
        )

        try:
            result = client.call_tool(
                "live_connect",
                {"address": address, "port": port, "alias": alias},
                timeout=15.0,
            )

            if isinstance(result, str) and result.startswith("Error"):
                logger.warning("Tracy live_connect failed: %s", result)
                return None

            logger.info("Tracy MCP connected to engine: %s", result[:100])
            return result

        except TracyConnectionError as exc:
            logger.warning(
                "Tracy live_connect failed: %s — "
                "is the engine built with TRACY_ENABLE?", exc
            )
            return None

    def cleanup(self) -> None:
        """Clean up resources (don't kill Tracy MCP — it may be shared)."""
        # We don't kill Tracy MCP on exit since it may be used by other tools
        # The PID file cleanup is handled by tracy_mcp.py itself
        pass


# ---------------------------------------------------------------------------
# Phase 3 — Profile Tools
# ---------------------------------------------------------------------------


class ProfileCollector:
    """Orchestrates the reload → profile → collect workflow.

    Uses BAR MCP to reload widgets/gadgets and Tracy MCP to collect
    zone stats filtered by a name prefix.
    """

    def __init__(
        self,
        bar_registry: BarToolRegistry,
        tracy_client: TracyHttpClient,
        tracy_instance_id: str,
    ):
        self._bar = bar_registry
        self._tracy = tracy_client
        self._instance_id = tracy_instance_id

    # ------------------------------------------------------------------
    # Tracy eval helpers
    # ------------------------------------------------------------------

    def _tracy_eval(self, code: str, timeout: float = 60.0) -> str:
        """Execute Python code against the Tracy Worker via eval tool."""
        result = self._tracy.call_tool(
            "eval",
            {"code": code, "instance_id": self._instance_id},
            timeout=timeout,
        )
        return str(result) if result is not None else ""

    def _get_zone_stats_snapshot(self) -> Dict[str, Any]:
        """Capture a snapshot of all zone stats keyed by source-location ID.

        Returns a dict: { srcloc_id: {count, total, min, max, avg} }
        Only includes zones that have been entered at least once.
        """
        code = """
import re
result = {}
for key, stats in ctx.get_all_zone_stats().items():
    m = re.search(r'<(\\d+)>$', key)
    if m:
        sid = int(m.group(1))
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

    def _get_zone_names_by_prefix(self, prefix: str) -> Dict[int, str]:
        """Get zone source-location IDs and display names matching a prefix.

        Returns a dict: { srcloc_id: display_name }
        Display name is the human-readable part before ' (addr)'.
        """
        code = f"""
import re
prefix = {prefix!r}
result = {{}}
seen = set()
for key, stats in ctx.get_all_zone_stats().items():
    if not key.startswith(prefix):
        continue
    m = re.search(r'<(\\d+)>$', key)
    if m:
        sid = int(m.group(1))
        if sid not in seen:
            seen.add(sid)
            # Extract display name (part before ' (addr)')
            display = key.split(' (')[0] if ' (' in key else key
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

    # ------------------------------------------------------------------
    # Core profiling workflow
    # ------------------------------------------------------------------

    def _reload_and_profile(
        self,
        reload_tool: str,
        name: str,
        duration: float = 5.0,
    ) -> Dict[str, Any]:
        """Execute the full profile workflow: snapshot before → reload → wait → snapshot after → diff.

        Args:
            reload_tool: BAR tool name ("widget_reload" or "gadget_reload")
            name: Widget/gadget name (also used as zone prefix for filtering)
            duration: Seconds to wait after reload for zone data to accumulate

        Returns:
            Dict with zone stats and metadata.
        """
        logger.info(
            "Starting profile: %s='%s', duration=%.1fs", reload_tool, name, duration
        )

        # Step 1: Take a before-snapshot (baseline)
        logger.debug("Capturing Tracy zone snapshot (before)…")
        before = self._get_zone_stats_snapshot()
        logger.debug("Before snapshot: %d zones captured", len(before))

        # Step 2: Reload the widget/gadget via BAR MCP
        logger.info("Reloading via BAR: %s(name='%s')", reload_tool, name)
        try:
            reload_result = self._bar.call_tool(reload_tool, {"name": name}, timeout=15.0)
            logger.info("BAR reload result: %s", reload_result[:200])
        except BarConnectionError as exc:
            raise BarConnectionError(
                f"Failed to reload {name} via BAR MCP: {exc}"
            ) from exc

        # Step 3: Wait for the game to run and generate zone data
        logger.info("Waiting %.1fs for zone data to accumulate…", duration)
        time.sleep(duration)
        logger.info("Wait period finished.")

        # Step 4: Capture after-snapshot
        logger.debug("Capturing Tracy zone snapshot (after)…")
        after = self._get_zone_stats_snapshot()
        logger.debug("After snapshot: %d zones captured", len(after))

        # Step 5: Get zone names matching prefix to filter SIDs
        relevant_zones = self._get_zone_names_by_prefix(name)

        # Step 6: Compute delta — only for zones that match the prefix and have new entries
        delta = self._compute_delta(before, after, name, relevant_zones)
        return delta

    @staticmethod
    def _compute_delta(
        before: Dict[str, Any],
        after: Dict[str, Any],
        prefix: str,
        relevant_zones: Dict[int, str],
    ) -> Dict[str, Any]:
        """Compute the delta between two zone stat snapshots, filtered by prefix.

        Only includes zones that are in relevant_zones and whose count increased.
        """
        zones: Dict[str, Dict[str, Any]] = {}

        for sid, display_name in relevant_zones.items():
            sid_str = str(sid)
            b = before.get(sid_str, {})
            a = after.get(sid_str, {})

            b_count = b.get("count", 0)
            a_count = a.get("count", 0)

            # Only include zones that have new entries (count increased)
            if a_count <= b_count:
                continue

            zones[sid_str] = {
                "name": display_name,
                "count": a_count - b_count,
                "total": a.get("total", 0) - b.get("total", 0),
                "min": a.get("min", 0),
                "max": a.get("max", 0),
                "avg": a.get("avg", 0),
            }

        # Format output
        result = {
            "prefix": prefix,
            "zone_count": len(zones),
            "zones": zones,
        }

        if not zones:
            logger.warning(
                "No Tracy zones found with new entries after profiling '%s' — "
                "did you instrument with tracy.ZoneBeginN('%s:...') / tracy.ZoneEnd()?",
                prefix, prefix,
            )

        return result

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def profile_widget(
        self, name: str, duration: float = 5.0
    ) -> str:
        """Profile a LuaUI widget: reload it, wait, collect zone stats.

        Zones are filtered by the widget name prefix.
        """
        result = self._reload_and_profile("widget_reload", name, duration)
        return self._format_result(result, "widget")

    def profile_gadget(
        self, name: str, duration: float = 5.0
    ) -> str:
        """Profile a LuaRules gadget: reload it, wait, collect zone stats.

        Zones are filtered by the gadget name prefix.
        """
        result = self._reload_and_profile("gadget_reload", name, duration)
        return self._format_result(result, "gadget")

    def profile_diff(
        self, name: str, duration: float = 5.0, reload_tool: str = "widget_reload"
    ) -> str:
        """Run two profile passes and return the delta.

        First pass profiles the current state, second pass profiles after
        a reload. The delta shows improvement or regression.
        """
        item_type = "gadget" if reload_tool == "gadget_reload" else "widget"
        logger.info(
            "Starting diff profile: %s='%s', duration=%.1fs", item_type, name, duration
        )

        # First pass
        logger.info("Diff profile — first pass")
        pass1 = self._reload_and_profile(reload_tool, name, duration)

        # Brief pause between passes
        time.sleep(0.5)

        # Second pass
        logger.info("Diff profile — second pass")
        pass2 = self._reload_and_profile(reload_tool, name, duration)

        # Compute delta between passes
        delta = self._compute_pass_delta(pass1, pass2, name)
        return self._format_diff_result(delta, item_type)

    @staticmethod
    def _compute_pass_delta(
        pass1: Dict[str, Any],
        pass2: Dict[str, Any],
        prefix: str,
    ) -> Dict[str, Any]:
        """Compute the delta between two profiling passes."""
        zones1 = pass1.get("zones", {})
        zones2 = pass2.get("zones", {})

        all_ids = set(zones1.keys()) | set(zones2.keys())
        delta_zones: Dict[str, Dict[str, Any]] = {}

        for sid_str in all_ids:
            z1 = zones1.get(sid_str, {})
            z2 = zones2.get(sid_str, {})

            c1 = z1.get("count", 0)
            c2 = z2.get("count", 0)
            t1 = z1.get("total", 0)
            t2 = z2.get("total", 0)

            count_diff = c2 - c1
            total_diff = t2 - t1

            # Calculate percentage change
            count_pct = (count_diff / c1 * 100) if c1 else 0
            total_pct = (total_diff / t1 * 100) if t1 else 0

            delta_zones[sid_str] = {
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
            "prefix": prefix,
            "zone_count": len(delta_zones),
            "zones": delta_zones,
        }

    # ------------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------------

    @staticmethod
    def _format_result(result: Dict[str, Any], item_type: str) -> str:
        """Format profile result as a readable string with JSON data."""
        prefix = result["prefix"]
        zone_count = result["zone_count"]
        zones = result["zones"]

        lines = [
            f"Profile result for {item_type} '{prefix}':",
            f"  Zones with new entries: {zone_count}",
            "",
        ]

        if zone_count:
            lines.append("  Zone stats (count, total_us, min_us, max_us, avg_us):")
            # Sort by total time descending
            sorted_zones = sorted(
                zones.items(), key=lambda kv: kv[1].get("total", 0), reverse=True
            )
            for sid, stats in sorted_zones[:50]:  # cap at 50 zones
                total_us = stats["total"] / 1e3
                min_us = stats["min"] / 1e3
                max_us = stats["max"] / 1e3
                avg_us = stats["avg"] / 1e3
                lines.append(
                    f"    [{sid}] count={stats['count']:>6}  "
                    f"total={total_us:>10.2f}  "
                    f"min={min_us:>8.2f}  "
                    f"max={max_us:>8.2f}  "
                    f"avg={avg_us:>8.2f}"
                )
            if len(zones) > 50:
                lines.append(f"    ... and {len(zones) - 50} more zones (see JSON below)")
            lines.append("")

        # Append full JSON for programmatic access
        lines.append("  Full JSON:")
        lines.append(json.dumps(result, indent=2))
        return "\n".join(lines)

    @staticmethod
    def _format_diff_result(result: Dict[str, Any], item_type: str) -> str:
        """Format diff profile result as a readable string."""
        prefix = result["prefix"]
        zone_count = result["zone_count"]
        zones = result["zones"]

        lines = [
            f"Diff profile result for {item_type} '{prefix}':",
            f"  Zones compared: {zone_count}",
            "",
        ]

        if zone_count:
            lines.append("  Zone delta (count_diff, total_diff_us, total_pct, avg_before_us, avg_after_us):")
            # Sort by absolute total percentage change
            sorted_zones = sorted(
                zones.items(),
                key=lambda kv: abs(kv[1].get("total_pct", 0)),
                reverse=True,
            )
            for sid, stats in sorted_zones[:50]:
                total_diff_us = stats["total_diff"] / 1e3
                avg_b_us = stats["avg_before"] / 1e3
                avg_a_us = stats["avg_after"] / 1e3
                direction = "↑" if stats["total_diff"] > 0 else "↓" if stats["total_diff"] < 0 else "→"
                lines.append(
                    f"    [{sid}] {direction} count={stats['count_diff']:>+6}  "
                    f"total_diff={total_diff_us:>10.2f}  "
                    f"pct={stats['total_pct']:>+7.1f}%  "
                    f"avg={avg_b_us:>8.2f} → {avg_a_us:>8.2f}"
                )
            if len(zones) > 50:
                lines.append(f"    ... and {len(zones) - 50} more zones (see JSON below)")
            lines.append("")

        lines.append("  Full JSON:")
        lines.append(json.dumps(result, indent=2))
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Phase 4 — Main Server Entrypoint
# ---------------------------------------------------------------------------


def _create_bar_tools_server(
    bar_client: BarTcpClient,
    bar_registry: BarToolRegistry,
    tracy_client: Optional[TracyHttpClient] = None,
    tracy_instance_id: Optional[str] = None,
) -> Any:
    """Create a FastMCP server with all BAR tools registered as MCP tools.

    Dynamically wraps each discovered BAR tool so the AI client can call them
    via standard MCP (stdio or SSE). Also registers Tracy pass-through tools
    if tracy_client is provided.
    """
    import mcp.server.fastmcp as fastmcp

    server = fastmcp.FastMCP("BAR+Tracy Bridge")

    # MCP handshake: initialize is a request (with id), then send initialized notification.
    # The Lua server replies using msg.id, so we must send a proper request and validate the response id.
    init_params = {
        "protocolVersion": "2025-11-25",
        "capabilities": {},
        "clientInfo": {"name": "bar_tracy_bridge", "version": "1.0.0"},
    }
    try:
        init_req_id = bar_client.send_jsonrpc("initialize", init_params)
        init_response = bar_client.receive_response(init_req_id, timeout=5.0)

        # Validate that the response id matches our request id
        if init_response.get("id") != init_req_id:
            logger.warning(
                "BAR MCP initialize response id mismatch: expected %s, got %s",
                init_req_id, init_response.get("id"),
            )

        # Check for JSON-RPC error in the response
        if "error" in init_response:
            logger.warning(
                "BAR MCP initialize error: %s",
                init_response["error"].get("message", "unknown"),
            )
        else:
            logger.info("BAR MCP initialized successfully (protocol: %s)", init_response.get("result", {}).get("protocolVersion"))

    except BarConnectionError as exc:
        logger.warning("No response to initialize request (non-fatal): %s", exc)

    # Now send the initialized notification (no id, no response expected)
    bar_client.send_notification("notifications/initialized")

    # Discover BAR tools
    try:
        bar_registry.discover()
    except BarConnectionError as exc:
        logger.error("Failed to discover BAR tools: %s", exc)

    # ------------------------------------------------------------------
    # Helper: build a Python type hint string from a JSON Schema type
    # ------------------------------------------------------------------
    def _schema_type_to_python(t: str) -> str:
        if t == "string":
            return "str"
        if t == "integer":
            return "int"
        if t == "number":
            return "float"
        if t == "boolean":
            return "bool"
        return "Any"

    # ------------------------------------------------------------------
    # Helper: build a default-value string from a JSON Schema property
    # ------------------------------------------------------------------
    def _schema_default(prop: Dict[str, Any], py_type: str) -> str:
        if "default" in prop:
            return json.dumps(prop["default"])
        # Heuristic defaults
        if py_type == "str":
            return '""'
        if py_type == "int":
            return "0"
        if py_type == "float":
            return "0.0"
        if py_type == "bool":
            return "False"
        return "None"

    # ------------------------------------------------------------------
    # Register each BAR tool as an MCP tool on the bridge server
    # ------------------------------------------------------------------
    # We generate a real Python function per tool with:
    #   - __name__ == tool_name  (so FastMCP picks up the correct name)
    #   - typed parameters from inputSchema  (so FastMCP exposes a real schema)
    #   - proper docstring
    # This avoids the "all tools named handler" bug and **kwargs schema loss.
    for tool_def in bar_registry.tools:
        tool_name = tool_def["name"]
        tool_desc = tool_def.get("description", "")
        input_schema = tool_def.get("inputSchema", {})
        properties = input_schema.get("properties", {})
        # Some tools may have properties as a list or missing — normalize to dict
        if not isinstance(properties, dict):
            properties = {}
        required = set(input_schema.get("required", []))

        # Build parameter list:  name: type = default
        # Required params must come before optional ones (Python syntax rule)
        params: List[str] = []
        annotations: Dict[str, str] = {}
        for pname, prop in properties.items():
            raw_type = prop.get("type", "string")
            py_type = _schema_type_to_python(raw_type)
            annotations[pname] = py_type
            default = _schema_default(prop, py_type)
            if pname in required:
                params.append((pname, f"{pname}: {py_type}", True))
            else:
                params.append((pname, f"{pname}: {py_type} = {default}", False))
        # Sort: required (True) first, then optional (False)
        params.sort(key=lambda x: (not x[2], x[0]))
        param_signatures = [p[1] for p in params]

        # Build source for a real function with typed signature
        param_str = ", ".join(param_signatures) if param_signatures else ""
        # Extract just param names to build the _args dict literal
        param_names = [p[0] for p in params]
        kwargs_expr = "{" + ", ".join(param_names) + "}" if param_names else "{}"

        func_source = f'''
def {tool_name}({param_str}) -> str:
    """{tool_desc}"""
    _args = {kwargs_expr}
    try:
        return bar_registry.call_tool({tool_name!r}, _args)
    except BarConnectionError as exc:
        logger.warning("BAR tool '%s' failed, attempting reconnect: %s", {tool_name!r}, exc)
        try:
            bar_client.reconnect()
            bar_registry.discover()
            return bar_registry.call_tool({tool_name!r}, _args)
        except BarConnectionError as exc2:
            raise BarConnectionError(
                f"BAR tool '{tool_name}' failed after reconnect: {{exc2}}"
            ) from exc2
'''
        try:
            namespace: Dict[str, Any] = {
                "bar_registry": bar_registry,
                "bar_client": bar_client,
                "BarConnectionError": BarConnectionError,
                "logger": logger,
            }
            exec(func_source, namespace)
            func = namespace[tool_name]

            # Set proper metadata
            func.__name__ = tool_name
            func.__doc__ = tool_desc
            func.__annotations__["return"] = str
            func.__annotations__.update(annotations)

            # Register with FastMCP using explicit name
            server.tool(tool_name, description=tool_desc)(func)
            logger.info("Registered BAR tool: %s (params: %s)", tool_name, param_str or "(none)")
        except Exception as reg_err:
            logger.error("Failed to register BAR tool '%s': %s\nSource:\n%s", tool_name, reg_err, func_source)

    # ------------------------------------------------------------------
    # Phase 2.3 — Pass-through Tracy tools
    # ------------------------------------------------------------------
    if tracy_client and tracy_client.connected:
        logger.info("Registering Tracy pass-through tools")

        # tracy_eval — execute Python code against a Tracy Worker
        @server.tool(
            description=(
                "Execute Python code against a Tracy Worker instance. "
                "The code runs with `ctx` bound to the Tracy Worker. "
                "Time values are in nanoseconds. Read tracy://eval-guide "
                "for the ctx object model."
            )
        )
        def tracy_eval(code: str, instance_id: Optional[str] = None) -> str:
            """Execute Python code against a Tracy Worker bound as `ctx`."""
            target_id = instance_id or tracy_instance_id
            if not target_id:
                return "Error: No Tracy instance_id. Call list_instances first."
            try:
                result = tracy_client.call_tool(
                    "eval",
                    {"code": code, "instance_id": target_id},
                    timeout=60.0,
                )
                return str(result) if result is not None else ""
            except TracyConnectionError as exc:
                raise TracyConnectionError(
                    f"Tracy eval failed: {exc}"
                ) from exc

        # list_instances — list loaded Tracy instances
        @server.tool(
            description="List all loaded Tracy instances and captures with metadata."
        )
        def tracy_list_instances() -> str:
            """List all Tracy instances."""
            try:
                result = tracy_client.call_tool("list_instances")
                return json.dumps(result) if result else "[]"
            except TracyConnectionError as exc:
                raise TracyConnectionError(
                    f"Tracy list_instances failed: {exc}"
                ) from exc

        # discover_instances — scan for running Tracy applications
        @server.tool(
            description=(
                "Scan for running Tracy-instrumented applications on local "
                "ports. Returns discovered ports that are listening."
            )
        )
        def tracy_discover_instances(port_range: str = "8086-8095") -> str:
            """Discover running Tracy instances."""
            try:
                result = tracy_client.call_tool(
                    "discover_instances", {"port_range": port_range}
                )
                return json.dumps(result) if result else "[]"
            except TracyConnectionError as exc:
                raise TracyConnectionError(
                    f"Tracy discover_instances failed: {exc}"
                ) from exc

        logger.info("Tracy tools registered: eval, list_instances, discover_instances")

        # ------------------------------------------------------------------
        # Phase 3 — Profile Tools (only available when both BAR + Tracy connected)
        # ------------------------------------------------------------------
        if tracy_instance_id:
            profile_collector = ProfileCollector(
                bar_registry, tracy_client, tracy_instance_id
            )

            @server.tool(
                description=(
                    "Profile a LuaUI widget: reload it, wait for zone data to accumulate, "
                    "then collect Tracy zone stats. Zones are matched by name prefix "
                    "(e.g., widget 'Example_Widget' matches zones named 'Example_Widget:*'). "
                    "The widget should be instrumented with tracy.ZoneBeginN('WidgetName:func') "
                    "/ tracy.ZoneEnd() for profiling to work."
                )
            )
            def profile_widget(name: str, duration: float = 5.0) -> str:
                """Profile a widget: reload → wait → collect zone stats."""
                return profile_collector.profile_widget(name, duration)

            @server.tool(
                description=(
                    "Profile a LuaRules gadget: reload it, wait for zone data to accumulate, "
                    "then collect Tracy zone stats. Zones are matched by name prefix. "
                    "The gadget should be instrumented with tracy.ZoneBeginN('GadgetName:func') "
                    "/ tracy.ZoneEnd() for profiling to work."
                )
            )
            def profile_gadget(name: str, duration: float = 5.0) -> str:
                """Profile a gadget: reload → wait → collect zone stats."""
                return profile_collector.profile_gadget(name, duration)

            @server.tool(
                description=(
                    "Run two profiling passes on a widget and return the delta. "
                    "Useful for measuring the impact of a code change. "
                    "First pass profiles current state, second pass profiles after reload. "
                    "The delta shows improvement (negative = faster) or regression."
                )
            )
            def profile_widget_diff(name: str, duration: float = 5.0) -> str:
                """Diff profile a widget: two passes with delta."""
                return profile_collector.profile_diff(
                    name, duration, reload_tool="widget_reload"
                )

            @server.tool(
                description=(
                    "Run two profiling passes on a gadget and return the delta. "
                    "Useful for measuring the impact of a code change. "
                    "First pass profiles current state, second pass profiles after reload. "
                    "The delta shows improvement (negative = faster) or regression."
                )
            )
            def profile_gadget_diff(name: str, duration: float = 5.0) -> str:
                """Diff profile a gadget: two passes with delta."""
                return profile_collector.profile_diff(
                    name, duration, reload_tool="gadget_reload"
                )

            logger.info("Profile tools registered: profile_widget, profile_gadget, profile_widget_diff, profile_gadget_diff")
        else:
            logger.info(
                "Tracy instance_id not available — profile tools not registered. "
                "Ensure Tracy MCP auto-connects successfully."
            )

    else:
        logger.info(
            "Tracy MCP not connected — Tracy tools and profile tools not registered."
        )

    return server


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

    # ------------------------------------------------------------------
    # Tracy MCP lifecycle (Phase 2.2)
    # ------------------------------------------------------------------
    tracy_auto = TracyAutoStart(host=TRACY_HOST, port=TRACY_PORT)
    tracy_client: Optional[TracyHttpClient] = None
    tracy_instance_id: Optional[str] = None

    if tracy_auto.ensure_running():
        tracy_client = TracyHttpClient(TRACY_HOST, TRACY_PORT)
        try:
            tracy_client.connect()
            # Auto-connect to the engine's Tracy server
            result = tracy_auto.auto_connect(
                tracy_client,
                address="127.0.0.1",
                port=8086,
                alias="live_engine",
            )
            # Extract instance ID from result message
            if result:
                # Result format: "Connected to live instance as 'live_engine'. ..."
                match = re.search(r"as '([^']+)'", str(result))
                if match:
                    tracy_instance_id = match.group(1)
                    logger.info("Tracy instance ID: %s", tracy_instance_id)
        except TracyConnectionError as exc:
            logger.warning("Tracy MCP connection failed (non-fatal): %s", exc)
            logger.warning("BAR tools will still work without Tracy")
            tracy_client = None
    else:
        logger.info("Tracy MCP not available — BAR tools only mode")

    # ------------------------------------------------------------------
    # BAR MCP connection
    # ------------------------------------------------------------------
    bar_client = BarTcpClient(BAR_HOST, BAR_PORT)
    bar_registry = BarToolRegistry(bar_client)

    try:
        bar_client.connect()
    except BarConnectionError as exc:
        logger.error("FATAL: %s", exc)
        logger.error(
            "Tip: Make sure the game is running with dev mode enabled "
            "(Spring.Utilities.IsDevMode() == true) and dbg_bar_mcp.lua is loaded."
        )
        sys.exit(1)

    # Create the FastMCP server with BAR + Tracy tools
    server = _create_bar_tools_server(
        bar_client, bar_registry, tracy_client, tracy_instance_id
    )

    # Run the server
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
        bar_client.disconnect()


if __name__ == "__main__":
    main()
