# Tracy Profiler — MCP & Python Reference

## Project Summary

**Tracy** is a real-time, nanosecond-resolution, remote telemetry, hybrid frame and sampling profiler for games and other applications.

- **Language:** C++11 (client), C++20 (profiler GUI), Python (bindings)
- **Capabilities:** CPU profiling for Python (via bindings).

## Architecture Overview

The MCP server and Python bindings rely heavily on the **Server library** (`server/`), which handles all data processing and file I/O.

```
┌─────────────────┐     ┌──────────────────┐
│   Python/MCP    │     │   Server Lib    │
│   Integration   │────>│   (server/)     │
│ (bindings/mcp)  │     │  Data processing │
└─────────────────┘     └──────────────────┘
```

## Relevant Directory Structure

### `server/` — Data Processing Library (Shared)

The MCP server and Python server bindings use the `TracyServer` static library.

| File | Role |
|------|------|
| `TracyWorker.hpp/.cpp` | **Central data processor** — Massive core class that ingests trace data, decompresses thread data, builds timeline events, manages zone/GPU/plot/memory/lock/symbol data. This is the core object exposed by the MCP server. |
| `TracyFileRead.hpp/.cpp` | **Capture file reader** — Memory-maps `.tracy` files, handles LZ4/ZSTD decompression streams, reads file headers and metadata. |

### `python/` — Python Bindings

pybind11-based Python bindings for both the **Tracy client** (instrumenting Python code) and the **Tracy server** (analyzing `.tracy` files from Python).

| File/Dir | Role |
|----------|---------|
| `bindings/Module.cpp` | **Client bindings** (`TracyClientBindings`). Exposes `is_enabled()`, `ColorType`, `PlotFormatType`, `frame_mark`, `alloc`, `free`, `message`, `plot`, `_plot_config`, `program_name`, `thread_name`, `app_info`, `frame_image`, `_ScopedZone`. |
| `bindings/ServerModule.cpp` | **Server bindings** (`TracyServerBindings`). Exposes `SourceLocation`, `ZoneStats`, `FrameStats`, `PlotSummary`, and full `tracy::Worker` API. |
| `bindings/ScopedZone.hpp` | `PyScopedZone` class wrapping `tracy::ScopedZone` with lazy construction. |
| `bindings/Memory.hpp` | `MemoryAllocate()` / `MemoryFree()` wrappers for Tracy's memory tracking. |
| `bindings/NameBuffer.hpp` | Thread-safe fixed-size buffer for caching string names. |
| `tracy_client/__init__.py` | Package entry point. |
| `tracy_client/tracy.py` | Python-level API: `Color`, `ScopedZone`, `ScopedFrame`, decorators, `plot_config()`. |
| `tracy_client/scoped.py` | Pure-Python context managers and decorators. |
| `tracy_client/TracyClientBindings.pyi` | Type stubs for C++ client bindings. |
| `pyproject.toml` | Build config via `scikit-build-core`, requires Python 3.10+. |

**Build Instructions:**
- **WSL (Ubuntu 24.04):** Install system dependencies (`python3.12-venv`, `pkg-config`, `libdbus-1-dev`, `libssl-dev`, `build-essential`, `cmake`, `ninja-build`), create a venv, install `scikit-build-core`, apply fixes to `CMakeLists.txt` (add `C` language support) and `pyproject.toml` (fix install directories), then `pip install .`.
- **Windows:** Install Visual Studio 2022 (C++ workload), CMake, and Python 3.10+. Create a venv in `python/`, install `scikit-build-core`, and run `pip install .`.

### `extra/mcp/` — Model Context Protocol server

| File | Purpose |
|------|---------|
| `tracy_mcp.py` | Python MCP server using `mcp.server.fastmcp`; exposes `ctx` (a `TracyServerBindings.Worker`) for querying zones, GPU zones, frames, threads, locks, memory. |
| `eval_guide.md` | Detailed guide on the `ctx` object model, units (nanoseconds), source-location ID joins, async query mode. |
| `start_mcp.sh` | Launcher script. |

## Key Files for Quick Reference

| What you need | File location |
|---------------|---------------|
| Python client bindings | `python/bindings/Module.cpp` |
| Python server bindings | `python/bindings/ServerModule.cpp` |
| Core data processor (used by MCP) | `server/TracyWorker.hpp` |
| File format reader (used by MCP) | `server/TracyFileRead.hpp` |
| MCP Server implementation | `extra/mcp/tracy_mcp.py` |
| MCP Object Model Guide | `extra/mcp/eval_guide.md` |
