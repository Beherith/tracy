# BAR + Tracy MCP Bridge

This directory contains a restart-resilient MCP bridge for agentic BAR Lua
development and Tracy profiling.

The bridge exposes a stable MCP server to the AI client, then supervises two
flaky local backends underneath it:

- BAR MCP over raw TCP: `127.0.0.1:23452`, implemented by `dbg_bar_mcp.lua`
- Tracy MCP over SSE/HTTP: `127.0.0.1:47380`, implemented by `tracy_mcp.py`

`tracy_mcp.py` is treated as an upstream backend. The resilience and dynamic
tool behavior live in `bar_tracy_bridge.py`.

## VS Code MCP Config

```json
{
  "servers": {
    "BAR": {
      "type": "stdio",
      "command": "N:\\github\\tracy\\extra\\mcp\\start_bridge.bat",
      "args": []
    }
  },
  "inputs": []
}
```

## Stable Bridge Tools

These tools are registered even when BAR, Tracy MCP, or the game engine are
offline:

- `bridge_status` - JSON health/status for BAR, Tracy MCP, Tracy engine, dynamic
  tool count, generations, and current config.
- `bridge_dynamic_tools` - currently installed BAR and `tracy_*` convenience
  wrappers.
- `bridge_reconnect` - force BAR, Tracy MCP, and/or Tracy engine reconnect.
- `bar_call_tool` - call any BAR tool by name with JSON arguments.
- `tracy_call_tool` - call any Tracy MCP tool by name with JSON arguments.
- `bar_refresh_tools` - rediscover BAR tools and replace dynamic wrappers.
- `tracy_refresh_tools` - rediscover Tracy tools and replace dynamic wrappers.
- `profile_widget`, `profile_gadget` - lazy reconnect, reload, wait, collect
  Tracy zones.
- `profile_widget_diff`, `profile_gadget_diff` - two lazy profiling passes and
  a delta report.

Dynamic convenience wrappers are also installed for discovered BAR tools and
Tracy tools. Tracy tools are prefixed with `tracy_`, for example `tracy_eval`.
The bridge advertises MCP `tools.listChanged` support and sends
`notifications/tools/list_changed` when rediscovery changes the dynamic wrapper
set. If a client does not honor those notifications, use the stable generic
tools instead.

BAR-side tools that can surface Lua/runtime issues, such as `lua_eval`,
`widget_reload`, `gadget_reload`, and `spring_command`, now lay down an infolog
marker before execution and return a structured JSON text report containing the
command result, console/infolog lines captured after the marker, and any
matching engine `Error...` lines found in that delta.

## Restart Behavior

The bridge starts even if BAR or Tracy are offline. Backend supervisors run in
the background with jittered backoff.

Before accepting MCP client requests, the bridge also performs a short BAR
startup probe. If BAR is already running, discovered BAR tools are installed in
time for the client's first `tools/list`. If the probe fails, startup continues
and the background supervisor keeps reconnecting.

When BAR reconnects:

1. TCP connect
2. MCP initialize
3. `tools/list`
4. install/replace BAR convenience wrappers

When Tracy reconnects:

1. start or connect to `tracy_mcp.py`
2. open SSE session
3. MCP initialize
4. `tools/list`
5. install/replace `tracy_*` convenience wrappers
6. live engine connection is established lazily when a profiling tool needs it

Profiling tools are always present. They reconnect BAR, Tracy MCP, and the live
engine instance at call time.

## Environment

Defaults can be overridden:

- `BAR_MCP_HOST` default `127.0.0.1`
- `BAR_MCP_PORT` default `23452`
- `TRACY_MCP_HOST` default `127.0.0.1`
- `TRACY_MCP_PORT` default `47380`
- `TRACY_ENGINE_HOST` default `127.0.0.1`
- `TRACY_ENGINE_PORT` default `8086`
- `TRACY_ENGINE_ALIAS` default `live_engine`
- `BRIDGE_STARTUP_BAR_PROBE` default `true`
- `BRIDGE_STARTUP_BAR_TIMEOUT` default `2.0`
- `BRIDGE_LOG_LEVEL` default `INFO`

Logs go to stderr and `bar_tracy_bridge.log`.

For BAR's in-engine infolog checks, the widget primarily captures lines through
`widget:AddConsoleLine`, which avoids waiting for `infolog.txt` to flush to
disk. If the marker is not present in the cached console lines, it falls back to
`VFS.LoadFile("infolog.txt")`.

## Profiling Convention

Instrument Lua with zones whose names start with the widget/gadget name:

```lua
tracy.ZoneBeginN("MyWidget:Update")
-- work
tracy.ZoneEnd()
```

Make sure every return path calls `tracy.ZoneEnd()`.

Then call:

```text
profile_widget("MyWidget", duration=5)
profile_gadget("MyGadget", duration=5)
```

## Validation

Useful local checks:

```powershell
python -B -m py_compile bar_tracy_bridge.py test_bridge_resilience.py
python -B -m unittest test_bridge_resilience.py
lua -e "assert(loadfile('dbg_bar_mcp.lua')); print('dbg_bar_mcp.lua syntax ok')"
```

The unit tests use fake BAR TCP and Tracy SSE backends to verify dynamic tool
rediscovery and Tracy restart recovery without touching the real engine.
