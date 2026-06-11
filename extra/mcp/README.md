# BAR + Tracy MCP Bridge

This directory contains a restart-resilient MCP bridge for agentic BAR Lua
development and Tracy profiling.

The bridge exposes a stable MCP server to the AI client, then supervises local
backends underneath it:

- BAR MCP over raw TCP: `127.0.0.1:23452`, implemented by `dbg_bar_mcp.lua`
- Tracy profiling in-process: lazy `TracyServerBindings` import plus direct
  live-engine/eval helpers folded into `bar_tracy_bridge.py`

`tracy_mcp.py` is no longer started as an upstream backend by the bridge. Tracy
binding import failures and live-engine connection failures are reported in
bridge status without preventing the MCP bridge from starting.

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

These tools are registered even when BAR, Tracy bindings, or the game engine are
offline:

- `bridge_status` - JSON health/status for BAR, local Tracy backend, Tracy
  engine, dynamic tool count, generations, and current config.
- `bridge_dynamic_tools` - currently installed BAR convenience wrappers and
  Tracy profile tools.
- `bridge_reconnect` - force BAR, local Tracy backend, and/or Tracy engine reconnect.
- `bar_call_tool` - call any BAR tool by name with JSON arguments.
- `bar_refresh_tools` - rediscover BAR tools and replace dynamic wrappers.
- `profile_zone_pattern` - appears only after the bridge has found and
  connected to a live Tracy engine; optionally reloads a widget/gadget, waits,
  then collects Tracy zones matching a Python regex.
- `profile_zone_pattern_diff` - appears only after the bridge has found and
  connected to a live Tracy engine; runs two profiling passes for the same zone
  pattern and an optional reload target.

Dynamic convenience wrappers are installed for discovered BAR tools. Local
Tracy tools are discovered for internal bridge operations, but raw Tracy tools
are not exposed to the MCP client. The two profile tools are dynamically
installed only while a live Tracy engine connection is ready.
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

1. create an in-process Tracy client
2. lazily import `TracyServerBindings`
3. discover the folded local Tracy tools
4. scan `TRACY_ENGINE_PORT_RANGE`, default `8086-8095`
5. try the configured `TRACY_ENGINE_PORT` first, then discovered ports
6. install `profile_zone_pattern` and `profile_zone_pattern_diff` only after a
   live engine connection succeeds

Profiling tools are hidden from MCP `tools/list` while no live Tracy engine is
connected. The background supervisor keeps scanning, and sends
`tools.listChanged` when those profile tools are added or removed. When
`reload_kind` is set, a profile call also reconnects BAR before calling
`widget_reload` or `gadget_reload`.

## Environment

Defaults can be overridden:

- `BAR_MCP_HOST` default `127.0.0.1`
- `BAR_MCP_PORT` default `23452`
- `TRACY_ENGINE_HOST` default `127.0.0.1`
- `TRACY_ENGINE_PORT` default `8086`
- `TRACY_ENGINE_PORT_RANGE` default `8086-8095`
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

Instrument Lua with stable, searchable zone names:

```lua
tracy.ZoneBeginN("MyWidget:Update")
-- work
tracy.ZoneEnd()
```

Make sure every return path calls `tracy.ZoneEnd()`.

Then call:

```text
profile_zone_pattern("^MyWidget:", duration=5)
profile_zone_pattern("^MyWidget:", duration=5, reload_kind="widget", reload_name="MyWidget")
profile_zone_pattern("^MyGadget:", duration=5, reload_kind="gadget", reload_name="MyGadget")
```

## Validation

Useful local checks:

```powershell
python -B -m py_compile bar_tracy_bridge.py test_bridge_resilience.py
python -B -m unittest test_bridge_resilience.py
lua -e "assert(loadfile('dbg_bar_mcp.lua')); print('dbg_bar_mcp.lua syntax ok')"
```

The unit tests use fake BAR TCP and local Tracy clients to verify dynamic tool
rediscovery and Tracy recovery without touching the real engine.
