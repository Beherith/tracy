# BAR-MCP-Bridge Server Test Strategy

This document outlines the test strategy for the tools and resources provided by the MCP server called: `BAR-MCP-Bridge`.

The task is to test each tool one by one. 

# Debug logs

Read these as needed if any MCP server calls result in oddiites.

The BAR MCP lua server ingame lives in `C:\Users\Peti\Documents\My Games\Spring\games\Beyond-All-Reason.sdd\luaui\Widgets\dbg_bar_mcp.lua`

The infolog from the BAR engine is here: `C:\Users\Peti\Documents\My Games\Spring\engine\recoil_2026.06.04\infolog.txt`

The bar_tracy_bridge.py logs here: `N:\github\tracy\extra\mcp\bar_tracy_bridge.log`



## Tool Test Cases

### 2. `list_instances`
**Purpose**: List live Tracy instances.
- [ ] **Initial State**: Call with no instances loaded. Verify it returns an empty list `[]`.
- [ ] **Connected State**: Connect to a live Tracy application. Call `list_instances` and verify the live instance appears in the list.

### 3. `discover_instances`
**Purpose**: Scan local ports for Tracy broadcasts.
- [ ] **No Active Clients**: Run with default port range. Verify it returns an empty list `[]`.
- [ ] **Active Client**: Start a Tracy client on port 8087. Run `discover_instances(port_range="8086-8088")`. Verify it returns the port 8087.

### 4. `live_connect`
**Purpose**: Connect to a live Tracy application.
- [ ] **Success Case**: Start a Tracy client on port 8086. Call `live_connect(port=8086)`. Verify it returns a success message with a generated alias.
- [ ] **Protocol Mismatch**: Start a Tracy client with a different protocol version. Verify it returns a "Protocol mismatch" error message.
- [ ] **Connection Refused**: Call `live_connect` on a port where no client is running. Verify it returns a "Handshake failed" error with a hint.

### 7. `eval`
**Purpose**: Execute Python code against a worker.
- [ ] **Sync Success**: Connect to a live Tracy application. Call `eval(code="ctx.get_stats()", instance_id="...")`. Verify it returns the result of the call.
- [ ] **Sync Error**: Call `eval(code="1/0", instance_id="...")`. Verify it returns the Python exception string.
- [ ] **Async Success**: Call `eval(code="ctx.get_stats()", instance_id="...", async_mode=True)`. Verify it returns a `task_id`.
- [ ] **Async Poll**: Use the `task_id` from the previous step to call `task(action="poll", task_id="...")`. Verify it returns the result once completed.

### 8. `task`
**Purpose**: Manage background tasks.
- [ ] **List**: Run an async `eval`. Call `task(action="list")`. Verify the task appears in the list with "running" status.
- [ ] **Poll**: Call `task(action="poll", task_id="...")` repeatedly until status is "completed".
- [ ] **Cancel**: Run an async `eval`. Call `task(action="cancel", task_id="...")`. Verify status changes to "cancelled".

### 9. `shutdown_server`
**Purpose**: Shut down the server process.
- [ ] **Execution**: Call `shutdown_server`. Verify the process terminates (the MCP client should lose connection).

### 10. `lua_eval`
**Purpose**: Execute Lua code in the unsynced LuaUI widget environment.
- [ ] **Success Case**: Provide a valid Lua string. Verify it returns the serialized result.
- [ ] **Runtime Error**: Provide Lua code that throws an error. Verify it returns the error message.

### 11. `lua_eval_synced`
**Purpose**: Execute Lua code in the synced LuaRules gadget environment.
- [ ] **Success Case**: Provide a valid Lua string. Verify it returns the result (requires devmode + cheats).
- [ ] **Async Behavior**: Verify it returns a task ID or handles the async response correctly.

### 12. `widget_list`
**Purpose**: List all known LuaUI widgets and their active state.
- [ ] **Success Case**: Call the tool and verify it returns a JSON list of widgets.

### 15. `widget_reload`
**Purpose**: Reload (disable then re-enable) a LuaUI widget by name.
- [ ] **Success Case**: Provide a valid widget name. Verify it returns a success message.

### 16. `spring_command`
**Purpose**: Send a Spring/Recoil engine console command.
- [ ] **Success Case**: Provide a valid command (e.g., "reloadshaders"). Verify it returns "sent: [command]".

### 17. `vfs_read`
**Purpose**: Read a file from the game's virtual file system (VFS).
- [ ] **Success Case**: Provide a valid VFS path. Verify it returns the file content.
- [ ] **Truncation**: Provide a file larger than 512 KB. Verify it is truncated with a notice.
- [ ] **Not Found**: Provide a non-existent path. Verify it returns a "not found" error.

### 18. `vfs_list`
**Purpose**: List files in a VFS directory matching a glob pattern.
- [ ] **Success Case**: Provide a valid VFS path and pattern. Verify it returns a JSON list of files.

### 19. `game_info`
**Purpose**: Return current game state.
- [ ] **Success Case**: Call the tool and verify it returns a JSON object with frame, map, mod, player, etc.

### 20. `gadget_list`
**Purpose**: List all known LuaRules gadgets and whether they are active.
- [ ] **Success Case**: Call the tool and verify it returns a JSON list of gadgets.

### 23. `gadget_reload`
**Purpose**: Reload (disable then re-enable) a LuaRules gadget by name.
- [ ] **Success Case**: Provide a valid gadget name. Verify it returns a success message.
