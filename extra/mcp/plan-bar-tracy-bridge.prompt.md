# Plan: BAR + Tracy MCP Bridge Server

**TL;DR:** Build a Python bridge MCP server (`bar_tracy_bridge.py`) that (1) solves the TCP transport problem by adapting BAR's raw-TCP JSON-RPC to standard MCP stdio/SSE, and (2) exposes `profile_zone_pattern` tools that orchestrate optional reload → profile → collect workflows across both MCP servers.

### Core Workflow

The primary use case is an iterative profiling loop:

```
Agent instruments Lua code with tracy.ZoneBeginN("Example_Widget:funcname") / ZoneEnd()
  ↓
Agent calls profile_zone_pattern("^Example_Widget:", duration=5) on the Bridge
  ↓
Bridge:
  1. Clear all Tracy zone stats (fresh measurement)
  2. Reload widget via BAR MCP
  3. Wait duration seconds (game runs, zones accumulate)
  4. Collect zone stats filtered to "Example_Widget:*"
  ↓
Bridge returns zone stats (name, count, total, min, max, avg)
  ↓
Agent optimizes code → re-profiles → compares
```

### Startup Flow

On bridge startup:
1. Check if Tracy MCP is running (PID file check)
2. If not running, auto-start `tracy_mcp.py` as a subprocess
3. Wait for Tracy MCP SSE endpoint to be ready
4. Auto-connect Tracy MCP to the engine's Tracy server via `live_connect`
5. Connect to BAR MCP via TCP
6. Dynamically discover and register all BAR tools

### Architecture

```
┌──────────────────────┐
│  AI Client (Copilot) │
│  (stdio or SSE)      │
└──────────┬───────────┘
           │ Standard MCP (stdio/SSE via FastMCP)
           ▼
┌──────────────────────────────────────────┐
│     bar_tracy_bridge.py                  │
│                                          │
│  ┌──────────────────┐  ┌──────────────┐ │
│  │ BAR TCP Adapter   │  │ Tracy Client │ │
│  │ (TCP↔JSON-RPC)    │  │ (HTTP→SSE)   │ │
│  └────────┬──────────┘  └──────┬───────┘ │
└───────────┼────────────────────┼─────────┘
            │                    │
    ┌───────▼──────┐    ┌───────▼────────┐
    │ BAR MCP TCP  │    │ Tracy MCP SSE  │
    │ 127.0.0.1    │    │ 127.0.0.1      │
    │ :23452       │    │ :47380         │
    └──────────────┘    └────────────────┘
```

### Logging & Error Reporting

The bridge needs robust, structured logging since it sits between 3 components and failures at any layer need to be diagnosable.

**Logger setup** (inside the single file):
- Python `logging` module with a dedicated `bar_tracy_bridge` logger
- Default output: stderr (doesn't interfere with stdio MCP transport)
- Log level controlled by env var `BRIDGE_LOG_LEVEL` (default: `INFO`, set to `DEBUG` to see all wire traffic)
- All log lines prefixed with `[BRIDGE]` for easy grepping

**What gets logged:**

| Level | BAR TCP | Tracy SSE | Profile Tools |
|-------|---------|-----------|---------------|
| `DEBUG` | Every JSON-RPC line sent/received (raw) | Every HTTP request/response body | Every Tracy `eval` code string and result |
| `INFO` | Connect/disconnect/reconnect events | Connect/disconnect, auto-start Tracy MCP | Profile start/end, zone count, total time summary |
| `WARNING` | Reconnect attempts (with attempt count) | Retry attempts for SSE endpoint | No zones found for prefix (possible instrumentation error) |
| `ERROR` | Connection refused, TCP errors, JSON parse failures | HTTP errors, SSE stream broken, Tracy MCP crash | Tracy eval timeout, widget reload failure |

**Descriptive error messages** — every failure path includes context:

- **BAR connection refused:** `"Cannot connect to BAR MCP on 127.0.0.1:23452 — is the game running with dev mode enabled? (Spring.Utilities.IsDevMode())"`
- **BAR JSON parse error:** `"Invalid JSON-RPC response from BAR MCP: {raw_line} — is dbg_bar_mcp.lua loaded? Check game console for '[BARMCP]' messages."`
- **BAR tool error (isError=true):** `"BAR MCP tool '{tool_name}' returned error: {text} — check game console for details."`
- **Tracy MCP not starting:** `"Tracy MCP failed to start within 30s — check that tracy_mcp.py exists at {path} and TracyServerBindings are built."`
- **Tracy live_connect failed:** `"Tracy MCP could not connect to engine at {addr}:{port} — is the engine built with TRACY_ENABLE? Error: {detail}"`
- **Tracy SSE stream broken:** `"Tracy MCP SSE connection lost — Tracy MCP process may have crashed. Check {pid_file}."`
- **Profile: no zones found:** `"No Tracy zones found matching '{prefix}*' after {duration}s profiling — did you instrument the widget with tracy.ZoneBeginN('{prefix}:...') / tracy.ZoneEnd()?"`
- **Profile: eval timeout:** `"Tracy eval timed out after {timeout}s — the trace may be very large or Tracy MCP is unresponsive."`

#### Phase 1: BAR TCP Adapter (solves the transport problem) ✅ DONE

**Implemented:** `bar_tracy_bridge.py` — Phase 1 complete.

**1.1 — `BarTcpClient` class** (inside the single file) ✅
- Persistent TCP connection to `127.0.0.1:23452`
- `send_jsonrpc(method, params, request_id)` — sends newline-terminated JSON-RPC 2.0
- `receive_response(timeout)` — reads until `\n`, buffers partial messages
- Reconnect on disconnect with exponential backoff
- Thread-safe with internal lock for socket operations
- **Notable implementation detail:** `receive_response()` uses a deadline-based loop with short (1s) socket timeouts so it can check the buffer after each recv without blocking indefinitely. Empty read = connection closed.

**1.2 — BAR tool forwarding** (inside the single file) ✅
- Dynamically discover all BAR tools by calling BAR's `tools/list` endpoint
- For each discovered tool, register a `@mcp_server.tool()` wrapper that:
  1. Sends the tool call to BAR via TCP JSON-RPC
  2. Translates BAR's `{content: [{type: "text", text: "..."}], isError: bool}` to a Python string
  3. Returns the result to the AI client
- **All 14 BAR tools are automatically forwarded** — no manual wrapping needed. If BAR adds new tools, the bridge picks them up on next `tools/list`.
- **Notable implementation detail:** Tool handlers are generated via `exec()` in a controlled namespace (not `eval()` of arbitrary code — the handler code is constructed from known-safe templates). Each handler includes auto-reconnect logic: on `BarConnectionError`, it attempts reconnect + rediscover before giving up.
- **Notable implementation detail:** On startup, the bridge sends `initialize` (with request id, waits for response) + `notifications/initialized` (no response) to BAR MCP, matching the MCP handshake protocol.

#### Phase 2: Tracy Integration ✅ DONE

**Implemented:** `bar_tracy_bridge.py` — Phase 2 complete.

**2.1 — Tracy HTTP client** (`TracyHttpClient`) ✅
- Connect to Tracy MCP server's SSE endpoint via `httpx`
- Handles SSE handshake: `GET /sse` → parse session_id from response → `POST /messages/?session_id=...`
- Parses SSE response format: `event: result\ndata: {...}\n\n`
- `send_request(method, params)` — generic JSON-RPC request/response
- `call_tool(tool_name, arguments)` — convenience wrapper for `tools/call`
- **Notable implementation detail:** The session_id is extracted via regex from the SSE handshake response. Each request reuses the same session_id (FastMCP SSE sessions are persistent).

**2.2 — Tracy auto-start** (`TracyAutoStart`) ✅
- On bridge startup, check if Tracy MCP is running via PID file (`tracy_mcp.pid`)
- If not running, spawn `tracy_mcp.py` as subprocess (uses `sys.executable`)
- Poll SSE endpoint until ready (timeout 30s, 0.5s intervals)
- Auto-connect to the engine's Tracy server by calling `live_connect` on the Tracy MCP
- Extracts instance ID from result message via regex (`as 'live_engine'` → `live_engine`)
- **Notable implementation detail:** `ensure_running()` returns bool (True/False) — the bridge degrades gracefully to BAR-only mode if Tracy MCP can't start. The process is NOT killed on bridge exit since it may be shared with other tools.

**2.3 - Internal Tracy tools**
- Tracy MCP tools are discovered and called internally by the bridge.
- `eval`, `list_instances`, and `live_connect` support profiling and reconnect logic.
- Raw Tracy tools are not exposed as `tracy_*` MCP tools on the bridge server.
- File/capture-oriented Tracy tools such as `load_capture`/`unload_capture` are intentionally hidden from the client-facing tool list.

#### Phase 3: Profile Tools (the core workflow) ✅ DONE

**Implemented:** `bar_tracy_bridge.py` — Phase 3 complete.

**3.1 — `ProfileCollector` class** ✅
- Single class that orchestrates the full reload → profile → collect workflow
- `_tracy_eval(code)` — executes Python code against Tracy Worker via `eval` tool
- `_get_zone_stats_snapshot()` — captures all zone stats keyed by source-location ID (count, total, min, max, avg) using `ctx.get_all_zone_stats()` with regex extraction of `<srcloc_id>` from zone keys
- `_reload_and_profile(reload_tool, name, duration)` — core workflow: snapshot before → reload via BAR → sleep → snapshot after → compute delta
- `_compute_delta(before, after, prefix)` — diff two snapshots; only zones with increased counts are reported (new zone entries since reload)
- **Notable implementation detail:** Uses delta-based comparison (before/after snapshots) instead of "clear all stats" — this avoids the need to reset Tracy state and naturally filters to only zones exercised during the profiling window
- **Notable implementation detail:** Zone stats are keyed by source-location ID (extracted via regex from zone key format `'name (addr)[arch] <srcloc_id>'`), not by name — this avoids ambiguity when the same function is instrumented at multiple call sites

**3.2 — Four MCP tools registered on the bridge server** ✅
- **`profile_zone_pattern(zone_pattern, duration=5.0, reload_kind="", reload_name="")`** — optionally reload a widget/gadget → wait → collect matching zone stats
- **`profile_zone_pattern_diff(zone_pattern, duration=5.0, reload_kind="", reload_name="")`** — two passes for a zone pattern, return delta (count diff, total diff, percentage change)
- Profile tools only register when both BAR and Tracy are connected with a valid instance_id

**3.3 — Output formatting** ✅
- Human-readable table sorted by total time descending, capped at 50 zones
- Times converted to microseconds for readability (raw Tracy values are nanoseconds)
- Full JSON appended for programmatic access
- Diff output includes direction arrows (↑↓→), percentage change, and before→after avg comparison

#### Phase 4: Entrypoint ✅ DONE

**Implemented:** `bar_tracy_bridge.py` + `start_bridge.sh` — Phase 4 complete.

**4.1 — Main server** (inside `bar_tracy_bridge.py`) ✅
- `FastMCP("BAR+Tracy Bridge")` with all tools registered
- `main()` function with argparse: `--transport stdio|sse`, `--host`, `--port`
- Startup sequence: Tracy auto-start → Tracy connect + auto-connect to engine → BAR connect → create server → run
- Graceful degradation: BAR-only mode if Tracy MCP unavailable, FATAL exit if BAR MCP unavailable
- Env vars: `BAR_MCP_HOST`, `BAR_MCP_PORT`, `TRACY_MCP_HOST`, `TRACY_MCP_PORT`, `BRIDGE_LOG_LEVEL`

**4.2 — Launcher** — `extra/mcp/start_bridge.sh` ✅
- Sets `PYTHONPATH` for `TracyServerBindings` (adjustable Release/Debug suffix)
- Sources `start_bridge.local.sh` for machine-local overrides (not committed)
- Exports default env vars with user-overridable fallbacks
- Passes all args through to bridge (`--sse`, `--stdio`, `--port`, etc.)
- **Simplified design:** Tracy MCP auto-start is handled inside the bridge (Phase 2.2), so the launcher just sets environment and execs the bridge

### New Files

| File | Role |
|------|------|
| `extra/mcp/bar_tracy_bridge.py` | **Single file** — TCP adapter, BAR tool forwarding, Tracy client, Tracy pass-through, profile tools, main entrypoint |
| `extra/mcp/start_bridge.sh` | Launcher script |

### Verification

1. **TCP adapter test:** Start BAR with MCP widget → connect bridge → call `game_info` → verify response matches BAR output
2. **Profile workflow test:** Instrument a widget with `tracy.ZoneBeginN("Test:func")` → call `profile_zone_pattern("^Test:", 3)` → verify zone stats are returned with correct pattern filter
3. **Reload+profile test:** Modify widget code → call `profile_zone_pattern("^Test:", 5, reload_kind="widget", reload_name="Test")` → verify widget was reloaded AND new zone stats collected
4. **Diff test:** Call `profile_diff("Test", 3)` twice with different code → verify delta shows improvement/regression

### Decisions (Resolved)

1. **Delta-based profiling (before/after snapshots)** — instead of clearing all zone stats, the bridge takes a before-snapshot, reloads, waits, takes an after-snapshot, and reports only zones with increased counts. This avoids the need to reset Tracy state and naturally filters to only zones exercised during the profiling window
2. **Bridge auto-starts Tracy MCP** — checks PID file, spawns subprocess if needed, waits for SSE endpoint, then auto-connects to the engine's Tracy server via `live_connect`
3. **Agent does all instrumentation** — manual `tracy.ZoneBeginN("Name:func")` / `ZoneEnd()` calls give the agent full control over granularity; bridge never injects zones automatically
4. **No file-based profiling** — bridge only works with live Tracy connections
5. **BAR TCP protocol is opaque** — bridge implements the exact JSON-RPC format from `dbg_bar_mcp.lua`, no Lua changes needed
6. **Tracy connected via HTTP/SSE** — bridge connects to Tracy's SSE endpoint, keeping processes independent
7. **Zone prefix convention** — zones use `"WidgetName:funcname"` format as per user's instrumentation pattern
8. **Profile tools are the primary value** — BAR tool passthrough is secondary; the `profile_zone_pattern` tools are the main reason the bridge exists
9. **Robust logging throughout** — all inter-component communication logged at DEBUG level; errors include descriptive messages with actionable guidance (e.g., "check game console for [BARMCP] messages", "is engine built with TRACY_ENABLE?")
10. **Logging to stderr** — doesn't interfere with stdio MCP transport; level controlled by `BRIDGE_LOG_LEVEL` env var
11. **Zone stats keyed by source-location ID** — snapshots use srcloc_id (extracted from zone key via regex `<\d+>$`) as the join key, not zone names, to correctly track zones across reloads when the source code changes
12. **Diff tools use two full passes** — `profile_zone_pattern_diff` runs the optional reload→wait→collect workflow twice, then computes per-zone delta (count diff, total diff, percentage change, avg before→after)
13. **Readable + JSON output format** — profile results include a human-readable table (sorted by total time, capped at 50 zones, times in μs) followed by full JSON for programmatic access

# Improvements:

After reviewing the design plan, the Lua server implementation, and the Python bridge, here is a detailed analysis of flaws and areas for improvement.

### 1. Architecture & Design

**The "PID File" Fallacy (`bar_tracy_bridge.py`)**
*   **Flaw:** `TracyAutoStart._is_running()` relies solely on the existence of a PID file. If `tracy_mcp.py` crashes or is killed forcefully, the PID file remains on disk, and the bridge will incorrectly believe the server is running, leading to connection failures.
*   **Improvement:** Implement a "liveness check." Instead of just checking for the file, the bridge should attempt to connect to the `TRACY_PORT` (e.g., via a simple TCP socket check or an HTTP GET to `/sse`). If the port is unreachable, it should ignore the PID file and restart the process.

**Fragile SSE Session Extraction (`bar_tracy_bridge.py`)**
*   **Flaw:** The `TracyHttpClient` likely uses regex to extract the `session_id` from the SSE handshake response. SSE streams can be interleaved with comments or heartbeats, making simple regex parsing fragile.
*   **Improvement:** Use a proper SSE parser or a more robust loop that specifically looks for the `event: endpoint` or `data:` lines containing the session ID, ensuring it handles multi-line responses correctly.

**Dynamic Tool Generation via `exec()` (`bar_tracy_bridge.py`)**
*   **Observation:** The plan mentions using `exec()` to dynamically create tool handlers based on `tools/list`. 
*   **Risk:** While clever and DRY, this makes debugging significantly harder (stack traces point to `<string>`) and bypasses static analysis/type checking.
*   **Improvement:** Use a generic "Dispatcher" handler. Instead of creating $N$ functions, create one function that takes the tool name as an argument and forwards it to the `BarTcpClient`. This achieves the same result without the overhead and risk of `exec()`.

---

### 2. Robustness & Error Handling

**TCP Framing & Buffer Bloat (dbg_bar_mcp.lua)**
*   **Flaw:** `sock:receive("*a")` reads all available data into a chunk. While the code correctly splits by `\n`, if a client sends a massive amount of data without newlines, the buffer could grow indefinitely.
*   **Improvement:** Implement a maximum buffer size for `c.buffer`. If the buffer exceeds (e.g.) 1MB without finding a newline, the connection should be dropped as a protocol violation to prevent memory exhaustion in the game process.

**Lack of Heartbeats/Keep-Alive (dbg_bar_mcp.lua)**
*   **Flaw:** There is no heartbeat mechanism between the Bridge and the Lua server. If the TCP connection enters a "half-open" state (common with some network configurations or crashes), the bridge might hang on `receive_response` until the OS timeout kicks in.
*   **Improvement:** Implement a simple `ping` tool in dbg_bar_mcp.lua and have the bridge send a heartbeat every 30 seconds to verify the connection is still alive.

**Blocking Profiling Windows (`bar_tracy_bridge.py`)**
*   **Flaw:** The `ProfileCollector` uses `time.sleep(duration)`. While `FastMCP` runs tools in threads, a long profiling duration (e.g., 10s) might cause the AI client (Copilot/Claude) to time out the tool call.
*   **Improvement:** Since MCP doesn't natively support "progress" updates for tool calls yet, the bridge should explicitly log the start and end of the sleep period to stderr so the user knows the agent isn't frozen.

---

### 3. Performance & Optimization

**Lua-side JSON Overhead (dbg_bar_mcp.lua)**
*   **Observation:** `Json.encode` is called on every response in the game loop.
*   **Improvement:** For frequently called tools (like `game_info`), consider using a simpler string concatenation format if the data is basic, or ensure the JSON library being used is the most optimized version available for the environment.

**Tracy Stats Extraction (`bar_tracy_bridge.py`)**
*   **Flaw:** The bridge extracts `srcloc_id` from zone keys using regex via an `eval` call to the Tracy Worker. This is computationally expensive and relies on a specific string format (`'name (addr)[arch] <srcloc_id>'`).
*   **Improvement:** If possible, modify the Tracy MCP server (`tracy_mcp.py`) to provide a structured `get_zone_stats` tool that returns a list of objects `{ name, srcloc_id, stats }` rather than requiring the bridge to parse raw strings via Python `eval`.

### Summary of Recommended Changes

| Component | Priority | Change |
| :--- | :--- | :--- |
| **Bridge** | High | Replace PID check with a TCP port liveness check. |
| **Bridge** | Medium | Replace `exec()` tool generation with a generic dispatcher. |
| **Lua Server** | Medium | Add a max buffer size to the TCP receiver. |
| **Lua Server** | Low | Implement a `ping` tool for connection heartbeats. |
| **Tracy MCP** | Low | Expose structured zone stats to avoid regex parsing in the bridge.
