import json
import queue
import socket
import threading
import time
import unittest
import urllib.parse
from types import SimpleNamespace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from bar_tracy_bridge import (
    BarBackendSupervisor,
    ToolListNotifier,
    TracyBackendSupervisor,
    _enable_dynamic_tool_notifications,
    create_bridge_server,
)


class FakeFastMCP:
    def __init__(self):
        self._tool_manager = type("ToolManager", (), {"_tools": {}})()

    def tool(self, name=None, description=None):
        def decorate(fn):
            self._tool_manager._tools[name or fn.__name__] = fn
            return fn

        return decorate


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class FakeBarServer:
    def __init__(self, port, tool_names):
        self.port = port
        self.tool_names = list(tool_names)
        self._stop = threading.Event()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", port))
        self._sock.listen(5)
        self._sock.settimeout(0.2)
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                pass
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass
        self._thread.join(timeout=1.0)

    def _run(self):
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except OSError:
                break
            except socket.timeout:
                continue
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        with conn:
            buf = b""
            while not self._stop.is_set():
                try:
                    chunk = conn.recv(4096)
                except OSError:
                    break
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not line:
                        continue
                    msg = json.loads(line.decode("utf-8"))
                    if "id" not in msg:
                        continue
                    response = self._dispatch(msg)
                    conn.sendall((json.dumps(response) + "\n").encode("utf-8"))

    def _dispatch(self, msg):
        method = msg.get("method")
        if method == "initialize":
            return {"jsonrpc": "2.0", "id": msg["id"], "result": {"protocolVersion": "2025-11-25"}}
        if method == "tools/list":
            tools = [
                {
                    "name": name,
                    "description": f"{name} tool",
                    "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}},
                }
                for name in self.tool_names
            ]
            return {"jsonrpc": "2.0", "id": msg["id"], "result": {"tools": tools}}
        if method == "tools/call":
            params = msg.get("params", {})
            name = params.get("name")
            args = params.get("arguments", {})
            if name not in self.tool_names:
                return {"jsonrpc": "2.0", "id": msg["id"], "error": {"code": -32601, "message": "unknown"}}
            text = "pong" if name == "ping" else args.get("text", name)
            result = {"content": [{"type": "text", "text": text}], "isError": False}
            return {"jsonrpc": "2.0", "id": msg["id"], "result": result}
        return {"jsonrpc": "2.0", "id": msg["id"], "error": {"code": -32601, "message": "unknown method"}}


class FakeTracyState:
    def __init__(self, tool_names):
        self.tool_names = list(tool_names)
        self.sessions = {}
        self.instances = []
        self.stop = threading.Event()


class FakeTracyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    @property
    def state(self):
        return self.server.state

    def do_GET(self):
        if self.path != "/sse":
            self.send_error(404)
            return
        session_id = str(time.time_ns())
        q = queue.Queue()
        self.state.sessions[session_id] = q
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(f"event: endpoint\ndata: /messages/?session_id={session_id}\n\n".encode())
        self.wfile.flush()
        while not self.state.stop.is_set():
            try:
                payload = q.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                self.wfile.write(f"event: message\ndata: {payload}\n\n".encode())
                self.wfile.flush()
            except OSError:
                break

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        session_id = qs.get("session_id", [""])[0]
        length = int(self.headers.get("Content-Length", "0"))
        msg = json.loads(self.rfile.read(length).decode("utf-8"))
        response = self._dispatch(msg)
        if session_id in self.state.sessions:
            self.state.sessions[session_id].put(json.dumps(response))
        self.send_response(202)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _dispatch(self, msg):
        method = msg.get("method")
        if method == "initialize":
            return {"jsonrpc": "2.0", "id": msg.get("id"), "result": {"protocolVersion": "2025-11-25"}}
        if method == "tools/list":
            tools = [
                {
                    "name": name,
                    "description": f"{name} tool",
                    "inputSchema": {"type": "object", "properties": {}},
                }
                for name in self.state.tool_names
            ]
            return {"jsonrpc": "2.0", "id": msg.get("id"), "result": {"tools": tools}}
        if method == "tools/call":
            params = msg.get("params", {})
            name = params.get("name")
            if name == "live_connect":
                self.state.instances = [{"id": "live_engine", "path": None, "mtime": None, "live": True}]
                text = "Connected to live instance as 'live_engine'."
            elif name == "list_instances":
                text = json.dumps(self.state.instances)
            elif name == "eval":
                text = "{}"
            else:
                text = name or ""
            result = {"content": [{"type": "text", "text": text}], "isError": False}
            return {"jsonrpc": "2.0", "id": msg.get("id"), "result": result}
        return {"jsonrpc": "2.0", "id": msg.get("id"), "result": {}}


class FakeTracyServer(ThreadingHTTPServer):
    allow_reuse_address = True

    def __init__(self, port, tool_names):
        super().__init__(("127.0.0.1", port), FakeTracyHandler)
        self.state = FakeTracyState(tool_names)
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self.state.stop.set()
        self.shutdown()
        self.server_close()
        self._thread.join(timeout=1.0)


class BridgeResilienceTests(unittest.TestCase):
    def test_fastmcp_advertises_tool_list_changed(self):
        server = create_bridge_server()
        notifier = ToolListNotifier()
        _enable_dynamic_tool_notifications(server, notifier)

        options = server._mcp_server.create_initialization_options()
        self.assertIsNotNone(options.capabilities.tools)
        self.assertTrue(options.capabilities.tools.listChanged)

    def test_tool_list_notifier_sends_list_changed_to_captured_session(self):
        try:
            import anyio
        except ImportError:
            self.skipTest("anyio is not installed")

        class FakeSession:
            def __init__(self):
                self.count = 0
                self.sent = threading.Event()

            async def send_tool_list_changed(self):
                self.count += 1
                self.sent.set()

        async def run_case():
            notifier = ToolListNotifier()
            session = FakeSession()
            notifier.capture_request_context(SimpleNamespace(session=session), mark_generation_seen=True)
            await anyio.to_thread.run_sync(lambda: notifier.notify("bar", ["ping"]))
            ok = await anyio.to_thread.run_sync(lambda: session.sent.wait(2.0))
            self.assertTrue(ok)
            self.assertEqual(session.count, 1)
            self.assertEqual(notifier.status()["pending_sessions"], 0)

        anyio.run(run_case)

    def test_bar_supervisor_recovers_and_updates_tool_wrappers(self):
        server = FakeFastMCP()
        notifier = ToolListNotifier()
        port = free_port()
        bar = BarBackendSupervisor(server, notifier, port=port)

        with self.assertRaises(Exception):
            bar.ensure_ready(timeout=0.1)

        fake = FakeBarServer(port, ["ping", "echo"])
        fake.start()
        self.addCleanup(fake.stop)
        bar.ensure_ready()
        self.assertEqual(bar.call_tool("echo", {"text": "hello"}), "hello")
        self.assertIn("echo", server._tool_manager._tools)

        fake.tool_names = ["ping", "new_tool"]
        names = bar.refresh_tools()
        self.assertIn("new_tool", names)
        self.assertIn("new_tool", server._tool_manager._tools)
        self.assertNotIn("echo", server._tool_manager._tools)

    def test_tracy_supervisor_recovers_and_updates_tool_wrappers(self):
        try:
            import httpx  # noqa: F401
        except ImportError:
            self.skipTest("httpx is not installed")

        server = FakeFastMCP()
        notifier = ToolListNotifier()
        port = free_port()
        tracy_server = FakeTracyServer(port, ["list_instances", "live_connect", "eval"])
        tracy_server.start()
        self.addCleanup(tracy_server.stop)

        tracy = TracyBackendSupervisor(server, notifier, port=port)
        tracy.ensure_mcp_ready()
        self.assertIn("tracy_eval", server._tool_manager._tools)
        self.assertEqual(tracy.ensure_engine_ready(), "live_engine")

        tracy_server.stop()
        time.sleep(0.1)
        replacement = FakeTracyServer(port, ["list_instances", "live_connect", "eval", "new_stat"])
        replacement.start()
        self.addCleanup(replacement.stop)
        tracy.mark_mcp_offline("test restart")
        names = tracy.refresh_tools()
        self.assertIn("new_stat", names)
        self.assertIn("tracy_new_stat", server._tool_manager._tools)


if __name__ == "__main__":
    unittest.main()
