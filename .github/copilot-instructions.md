# Tracy Profiler — Repository Map for Coding Agents

## Project Summary

**Tracy** is a real-time, nanosecond-resolution, remote telemetry, hybrid frame and sampling profiler for games and other applications.

- **Author:** wolfpld (Bartosz Taudul)
- **Version:** ~0.13.x (check `public/common/TracyVersion.hpp` for exact version)
- **Language:** C++11 (client), C++20 (profiler GUI)
- **Build systems:** CMake 3.10+, Meson 1.3+
- **Protocols:** UDP broadcast for discovery, UDP/TCP for data transfer
- **Wire protocol version:** 78 (see `public/common/TracyProtocol.hpp`)

### Capabilities
- **CPU profiling:** C, C++, C++, Lua, Python, Fortran (direct); Rust, Zig, C#, OCaml, Odin (third-party bindings)
- **GPU profiling:** OpenGL, Vulkan, Direct3D 11/12, Metal, OpenCL, CUDA, ROCm
- **Memory allocation tracking, lock contention profiling, context switch capture, frame image/screenshot attribution, time-series plots, messaging

## Architecture Overview

Tracy has a **three-tier architecture**:

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Public API     │     │   Profiler GUI    │     │   Server Lib    │
│  (public/)      │     │  (profiler/)     │     │   (server/)     │
│ Client library   │────>│  Dear ImGui app  │<────│  Data processing │
│ Instrumentation  │     │  Visualization   │     │  File I/O       │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

1. **Public API** (`public/`) — Header-only instrumentation with zero overhead when disabled. Single-file integration via `TracyClient.cpp`.
2. **Profiler GUI** (`profiler/`) — Standalone Dear ImGui application with multi-platform backends (GLFW, Wayland, Emscripten/WASM).
3. **Server library** (`server/`) — Static library (`TracyServer`) that handles all data processing: file I/O with LZ4/ZSTD compression, broadcast discovery, thread ID compression, parallel task dispatch.

---

## Directory Structure

### `public/` — Client Library (The code linked into profiled applications)

#### `public/TracyClient.cpp`
- **Single aggregation file** that `#include`s all implementation `.cpp` files inline.
- This is the **only file** a user needs to compile to integrate Tracy.
- When `TRACY_ENABLE` is undefined, all macros become no-ops.

#### `public/TracyClient.F90`
- Fortran bindings (optional, enabled via `TRACY_Fortran` CMake option).

#### `public/tracy/` — Public API Headers

| Header | Purpose |
|--------|---------|
| `Tracy.hpp` | **C++ API** — Primary entry point. `ZoneScoped*`, `TracyMessage*`, `TracyPlot`, `TracyAlloc*`, `FrameMark*`, `TracyLockable*`, fiber macros. |
| `TracyC.h` | **C API** — Mirror for pure-C codebases. `___tracy_*` functions, `TracyC*` macros. |
| `TracyOpenGL.hpp` | OpenGL GPU profiling — `TracyGpuContext`, `TracyGpuZone`, `TracyGpuCollect` |
| `TracyVulkan.hpp` | Vulkan GPU profiling — `TracyVkContext`, `TracyVkZone`, `TracyVkCollect` |
| `TracyCUDA.hpp` | CUDA GPU profiling (via CUPTI) — `TracyCUDAContext`, `TracyCUDACollect` |
| `TracyD3D11.hpp` / `TracyD3D12.hpp` | Direct3D GPU profiling |
| `TracyOpenCL.hpp` | OpenCL GPU profiling |
| `TracyMetal.hmm` | Metal GPU profiling (Apple) |
| `TracyLua.hpp` | Lua scripting integration |

#### `public/client/` — Internal Client Headers

| File | Role |
|------|------|
| `TracyProfiler.hpp/.cpp` | **Core profiler singleton** — `tracy::Profiler` class manages concurrent queue (moodycamel), sends data via UDP, handles hardware timers (RDTSC/CNTVCT), callstack capture, memory tracking, GPU contexts |
| `TracyScoped.hpp` | `ScopedZone` RAII class — constructor sends `ZoneBegin`, destructor sends `ZoneEnd` |
| `TracyCallstack.hpp/.cpp` | Callstack unwinding via frame pointers and libbacktrace |
| `TracyRingBuffer.hpp` | Lock-free ring buffer for data transmission |
| `tracy_concurrentqueue.h` | Fork of moodycamel's ConcurrentQueue (lock-free MPMC) |
| `tracy_SPSCQueue.h` | Lock-free SPSC queue |
| `tracy_rpmalloc.cpp/.hpp` | Fork of rpmalloc for the profiler's own allocations |
| `TracyMangle.hpp` | Itanium/Microsoft demangler |
| `TracySysPower.cpp` | System power/frequency info |

#### `public/common/` — Shared Protocol & Data Structures

| File | Role |
|------|------|
| `TracyProtocol.hpp` | **Wire protocol** — Protocol version 78, broadcast version 3. All packet structs: `WelcomeMessage`, `BroadcastMessage`, `ServerQuery`, queue item types |
| `TracyQueue.hpp` | **Queue item types** — `enum class QueueType` with 80+ event types (ZoneBegin, ZoneEnd, Message, LockWait, MemAlloc, GpuZoneBegin, PlotData, etc.) |
| `TracySocket.hpp/.cpp` | Cross-platform UDP/TCP socket abstraction |
| `TracySystem.hpp/.cpp` | OS-specific utilities (thread naming, etc.) |
| `TracyMutex.hpp` | Cross-platform mutex |
| `tracy_lz4.cpp/.hpp` | LZ4 compression (forked) |

#### `public/libbacktrace/`
- Fork of Google's libbacktrace for stack trace symbol resolution.

---

### `server/` — Data Processing Library (Shared between GUI and tools)

Built as the static library `TracyServer`.

#### Core Modules

| File | Role |
|------|------|
| `TracyWorker.hpp/.cpp` | **Central data processor** — Massive core class that ingests trace data, decompresses thread data, builds timeline events, manages zone/GPU/plot/memory/lock/symbol data. Has `LoadProgress` for incremental loading, `ImportEvent*` structs for programmatic access |
| `TracyFileRead.hpp/.cpp` | **Capture file reader** — Memory-maps `.tracy` files, handles LZ4/ZSTD decompression streams, reads file headers and metadata |
| `TracyFileWrite.hpp/.cpp` | **Capture file writer** — Writes compressed trace files with 4 compression modes (Fast, Slow, Extreme, ZSTD) |
| `TracyFileHeader.hpp` | File magic numbers: `tracy` header, `tlZ4` (LZ4), `tZst` (ZSTD) |
| `TracyBroadcast.hpp/.cpp` | UDP broadcast parsing for client discovery |
| `TracyEvent.hpp` | Event data structures — `StringRef`, `StringIdx`, all event types parsed from the wire protocol |
| `TracyTaskDispatch.hpp/.cpp` | Thread pool for parallel data processing |
| `TracyThreadCompress.hpp/.cpp` | Compresses 64-bit thread IDs to 16-bit indices for bandwidth efficiency |
| `TracyMmap.hpp/.cpp` | Cross-platform memory mapping (POSIX mmap / Windows) |
| `TracyTextureCompression.hpp/.cpp` | DXT1 texture decompression for frame images |
| `TracyPrint.hpp/.cpp` | Utility printing functions |
| `TracySysUtil.hpp/.cpp` | System utilities (hostname, etc.) |
| `TracyMemory.hpp/.cpp` | Memory usage tracking |

#### Embedded Libraries (third-party, vendored)

| File | Origin |
|------|--------|
| `tracy_pdqsort.h` | Pattern-defeating quicksort |
| `tracy_robin_hood.h` | robin-hood hashing (unordered map) |
| `tracy_xxhash.h` | xxHash |

#### Data Flow (Server-side)
1. **Live capture**: Client sends UDP packets → `TracyWorker` receives and processes events in real-time → `TracyFileWrite` streams to disk
2. **File replay**: `TracyFileRead` memory-maps `.tracy` file → decompresses → `TracyWorker` ingests events → builds in-memory timeline
3. **Event types processed**: Locks, Messages, Plots, Memory, FrameImages, ContextSwitches, Samples, SymbolCode, SourceCache

---

### `profiler/` — Profiler GUI Application

Dear ImGui-based standalone application with multi-platform backends.

#### Structure

```
profiler/
  CMakeLists.txt          # Build config (C++20, CMake 3.25+)
  src/
    main.cpp              # Application entry point — ImGui + platform backends
    Backend.hpp           # Abstract window backend (GLFW/Wayland/X11/Emscripten)
    BackendGlfw.cpp       # GLFW backend (Windows/Linux)
    BackendWayland.cpp    # Wayland backend (Linux)
    BackendEmscripten.cpp # Web/WASM backend
    Fonts.cpp             # Font loading (Fira Code, Roboto, Noto Emoji, FontAwesome)
    Filters.cpp           # Timeline filtering logic
    HttpRequest.cpp       # HTTP server for web mode
    ImGuiContext.cpp      # Dear ImGui context management
    RunQueue.cpp          # Command queue for UI thread
    ConnectionHistory.cpp # Client connection history
    profiler/             # ~80 source files for the UI
    llm/                  # LLM integration (AI-assisted profiling)
  wasm/                   # WebAssembly build
  win32/                  # Windows-specific code
```

#### Key UI Modules

| Module | Purpose |
|--------|---------|
| `TracyView.*` | Main UI view controller — timeline rendering, navigation, selection |
| `TracyView_Timeline.cpp` | Timeline rendering engine |
| `TracyView_FlameGraph.cpp` | Flame graph visualization |
| `TracyView_CpuData.cpp` | CPU utilization view |
| `TracyView_GpuTimeline.cpp` | GPU timeline view |
| `TracyView_Memory.cpp` | Memory allocation tracking view |
| `TracyView_Messages.cpp` | Log messages view |
| `TracyView_Plots.cpp` | Time-series plots view |
| `TracyView_Locks.cpp` | Lock contention view |
| `TracyView_Callstack.cpp` | Callstack viewer |
| `TracyView_Samples.cpp` | CPU sampling view |
| `TracyView_Statistics.cpp` | Statistical analysis |
| `TracyTimelineController.*` | Timeline state management |
| `TracyTimelineItem.*` | Timeline item abstractions (CPU, GPU, plots, threads) |
| `TracyStorage.*` | Data storage/management layer |
| `TracySourceView.*` | Source code viewer |
| `TracySourceTokenizer.*` | Syntax highlighting |
| `TracyDisassembly.*` | Disassembly viewer |
| `TracyLlm.*` | AI-powered profiling assistant (LLM integration) |
| `TracyWeb.*` | HTTP server for browser-based viewing |
| `TracyConfig.*` | Application settings persistence |

---

### `capture/` — Trace Capture Tools

CLI tools to connect to a running Tracy-instrumented application and capture profiling data into a `.tracy` trace file.

| Executable | Source | Role |
|---|---|---|
| `tracy-capture` | `src/capture.cpp` | One-shot capture of a single profiling session. Options: `-o` output file, `-a` address, `-p` port, `-f` force overwrite, `-s` seconds (timed capture), `-m` memory limit (% of RAM). |
| `tracy-capture-daemon` | `src/capturedaemon.cpp` | Long-running daemon that listens for broadcast connections from any Tracy client and auto-captures. Supports `--filter-name` and `--filter-port`. |
| | `src/CaptureOutput.cpp` | Shared output helpers: terminal detection, ANSI color printing, `WaitForConnection()`, `PrintWorkerFailure()`, `PrintCaptureProgress()`. |

**Dependencies:** Links against `TracyServer` and `TracyGetOpt`.

---

### `csvexport/` — CSV Statistics Export Tool

CLI tool to extract profiling statistics from a `.tracy` trace file into CSV format.

| File | Role |
|------|------|
| `src/csvexport.cpp` | Reads `.tracy` file via `tracy::FileRead` + `tracy::Worker`, extracts zone statistics as CSV. Options: `-f`/`--filter` (filter zone names), `-s`/`--sep` (CSV separator), `-c`/`--case` (case-sensitive filter), `-e`/`--self` (self times), `-u`/`--unwrap` (per-CPU per-zone events), `-g`/`--gpu` (GPU zone events), `-m`/`--messages`, `-p`/`--plot`, `-t`/`--truncated_mean`. |

**Key functions:**
- `percentile_and_truncated_mean()` — Computes percentile value and truncated mean for outlier-resistant statistics.
- `GetZoneChildTimeFast()` — Calculates sum of children's durations for self-time computation.

---

### `monitor/` — External Process Profiler (Linux-only)

Attaches Tracy sampling profiling to an **external** process (NOT compiled with Tracy) using `ptrace` and Linux `perf_event`s.

| File | Role |
|------|------|
| `src/monitor.cpp` | Two modes: (1) **Launch mode** — starts a target program under `ptrace`. (2) **Attach mode** (`-p PID`) — attaches to an already-running process. Uses `perf_event` syscalls. Includes permission checks for `perf_event_paranoid` and `CAP_PERFMON`/`CAP_SYS_PTRACE`. |

**Dependencies:** Links against `TracyClient.cpp` directly (NOT the server). Linux-only.

---

### `merge/` — Multi-Trace Merge Tool

CLI tool to merge multiple `.tracy` trace files into a single trace file.

| File | Role |
|------|------|
| `src/merge.cpp` | Reads multiple `.tracy` input files, extracts timeline events, messages, plots, thread names, merges into unified trace. Usage: `tracy-merge -o output.tracy input1.tracy [input2.tracy ...]`. |

**Key data structures:**
- **`ExportedTrace`** — Represents a single trace file's data. `fromFile()` reads via `tracy::Worker`.
- **`MergedTrace`** — The merged result. `merge()` combines multiple traces, re-encodes thread IDs to be unique, renames threads/plots to disambiguate.

---

### `import/` — External Trace Format Importers

CLI tools to import profiling traces from **other profilers** into Tracy's `.tracy` format.

| Executable | Source | Role |
|---|---|---|
| `tracy-import-chrome` | `src/import-chrome.cpp` | Imports Chrome tracing format (`.json` / `.json.zst`) into `.tracy`. Supports phases: `b/B/e/E`, `X`, `i/I`, `C`, `M`. Uses `nlohmann_json` and Zstd. |
| `tracy-import-fuchsia` | `src/import-fuchsia.cpp` | Imports Fuchsia trace format (`.json` / `.json.zst`) into `.tracy`. Parses Fuchsia's compact binary JSON format. |

---

### `python/` — Python Bindings

pybind11-based Python bindings for both the **Tracy client** (instrumenting Python code) and the **Tracy server** (analyzing `.tracy` files from Python).

| File/Dir | Role |
|----------|------|
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
| `CMakeLists.txt` | Fetches pybind11 v2.13.6, builds two shared modules. |

---

### `dtl/` — Diff Template Library

Header-only C++ template library for computing diffs between sequences (by Tatsuhiko Kubo, BSD license). Entry point: `dtl.hpp`.

| Header | Purpose |
|---|---|
| `dtl.hpp` | **Main umbrella header** — includes all other headers. |
| `Diff.hpp` | **Two-way diff** — `dtl::Diff<elem, sequence, comparator>` (Myers' algorithm). |
| `Diff3.hpp` | **Three-way diff** — `dtl::Diff3<...>` for merge conflict detection. |
| `Lcs.hpp` | **Longest Common Subsequence** — `dtl::Lcs<elem>`. |
| `Ses.hpp` | **Shortest Edit Script** — `dtl::Ses<elem>`. |
| `Sequence.hpp` | **Base sequence class** — `dtl::Sequence<elem>`. |
| `functors.hpp` | **Functors** — `Printer` class template, comparator functors. |
| `variables.hpp` | **Type definitions** — `using` declarations. |

---

### `extra/` — Utility Tools and Data Files

#### Color generation tools
| File | Purpose |
|------|---------|
| `color.cpp` | Generates 256-entry color lookup table using sRGB↔linear conversions. |
| `color-hot.cpp` | Generates "hot" color gradient table. |

#### DXT texture compression tables
| File | Purpose |
|------|---------|
| `dxt1table.c` | Pre-generated DXT1 texture compression lookup table. |
| `dxt1divtable.c` | Division table for DXT1 compression. |
| `rdotbl.c` | R-dot table for DXT color endpoint selection. |

#### Other utilities
| File | Purpose |
|------|---------|
| `identify.cpp` | Reads a Tracy trace file header and prints its version. |
| `version.cpp` | Dumps Tracy file version as 4-byte binary blob to stdout. |
| `x11_colors.c` | X11 color name-to-RGB mappings. |
| `natvis.py` | **LLDB synthetic type formatters** for `TracyVector`, `ShortPtr`, etc. |
| `make-build.sh` | Shell script for build automation. |
| `update-meson-version.sh` | Script to sync version across meson build files. |

#### `extra/desktop/` — Linux desktop integration
- `tracy.desktop` — `.desktop` entry (MIME type `application/tracy`).
- `application-tracy.xml` — MIME type definition for `.tracy` files.

#### `extra/mcp/` — Model Context Protocol server
- `tracy_mcp.py` — Python MCP server using `mcp.server.fastmcp`; exposes `ctx` (a `TracyServerBindings.Worker`) for querying zones, GPU zones, frames, threads, locks, memory.
- `eval_guide.md` — Detailed guide on the `ctx` object model, units (nanoseconds), source-location ID joins, async query mode.
- `start_mcp.sh` — Launcher script.

#### `extra/uarch/` — CPU microarchitecture database
- `TracyMicroArchitecture.hpp` — Defines `AsmDesc`, `AsmVar`, `AsmOp`, `MicroArchitecture` structs for encoding x86 instruction latency/throughput/port data.
- `uarch.cpp` — The actual microarchitecture data tables.

---

### `examples/` — Example Projects

| Example | Description |
|---------|-------------|
| `fibers.cpp` | Demonstrates **Tracy fiber profiling** — `TracyFiberEnter`/`TracyFiberLeave` and custom zone contexts. Compiles with `-DTRACY_FIBERS`. |
| `CUDAGraphRepro/` | CUDA Graph regression test demonstrating GPU zone visibility issues with `cudaGraphLaunch`. |
| `OpenCLVectorAdd/` | Complete **OpenCL vector addition** example instrumented with Tracy (`TracyCLCtx`, `TracyOpenCL.hpp`). |
| `ToyPathTracer/` | Modified **Aras Pranckevičius's ToyPathTracer** — ray tracer used as a heavy workload test. |

---

### `test/` — Test Suite

| File | Role |
|------|------|
| `CMakeLists.txt` | Project: `tracy-test`, C/C++11, optional Lua support (`TRACY_HAS_LUA`). |
| `test.cpp` | **Comprehensive integration test** (~500+ lines) exercising nearly every Tracy feature across 25 threads: zone profiling, memory tracking, lock profiling, plotting, messaging, callstack capture, depth profiling, arena allocator tracking, image capture, Lua integration, signal handling, static initialization. |
| `stb_image.h` | Single-header image loader (used for screenshot tests). |
| `image.jpg` | Test image for screenshot capture. |

---

### `manual/` — Documentation

| File | Purpose |
|------|---------|
| `tracy.md` | **Main user manual** in Markdown — quick overview, first steps, client markup (C API), capturing data, analyzing data, CSV export, importing data, configuration files. |
| `tracy.tex` | LaTeX source for the same manual (PDF output). |
| `tracy.bib` | BibTeX bibliography file. |
| `techdoc.tex` | Technical documentation (architecture internals). |
| `latex2md.sh` | Script to convert LaTeX to Markdown. |
| `filter.lua` | Lua filter (likely for Pandoc processing). |
| `icons/` | Mouse button icons: `lmb.svg/pdf`, `mmb.svg/pdf`, `rmb.svg/pdf`, `scroll.svg/pdf`, `mouse.svg`. |
| `images/` | Screenshots: `screenshot-hi.png`, `screenshot-lo.png`, `ryzen.png`. |

---

### `doc/` — Documentation Assets

Contains three profiler screenshot images used for the README and documentation:
- `profiler.png`, `profiler2.png`, `profiler3.png`

---

### `cmake/` — Build Configuration

| File | Purpose |
|------|---------|
| `version.cmake` | Parses version from `public/common/TracyVersion.hpp` |
| `options.cmake` | Reusable `set_option()` / `set_option_value()` macros |
| `vendor.cmake` | Third-party dependency management via **CPM.cmake** — pulls Capstone, GLFW, FreeType, etc. from GitHub |
| `server.cmake` | Defines `TracyServer` static library (server-side code) |
| `CPM.cmake` | CMake Package Manager for fetching dependencies |
| `FindWaylandScanner.cmake` | Wayland scanner finder |
| `ECMFindModuleHelpers.cmake` | KDE ECM helper utilities |
| `GitRef.cmake` | Git reference utilities |
| `config.cmake` | Package configuration template |
| `*.patch` | Patches for vendored dependencies (imgui, ppqsort, tidy-cmake, gl3w) |

---

### `getopt/` — Portable getopt Implementation

BSD-licensed portable implementation of `getopt`/`getopt_long` by Kim Grasman. Included because Windows doesn't ship with `getopt_long`.

| File | Role |
|------|------|
| `getopt.h` | Declares `getopt()`, `getopt_long()`, `getopt_long_only()`, `option` struct. |
| `getopt.c` | Full implementation conforming to POSIX/FreeBSD/GNU extensions. |

---

### `icon/` — Application Icons

| File | Purpose |
|------|---------|
| `icon.svg` | Tracy's logo (vector, primary source). |
| `icon.png` | Raster version of the logo. |
| `icon.ico` | Windows icon file (multi-size). |
| `icon.pdf` | PDF version (used in the LaTeX manual). |
| `application-tracy.svg` | Desktop application icon (for Linux MIME integration). |

---

### `update/` — Update Tooling

Contains CMakeLists.txt and src/ directory for update/upgrade tooling.

---

### `.github/` — GitHub Configuration

| File | Purpose |
|------|---------|
| `FUNDING.yml` | GitHub Sponsors configuration |
| `sponsor.png` | Sponsor badge image |
| `workflows/` | GitHub Actions CI/CD workflows |

---

## Build Systems

### CMake (`CMakeLists.txt`)
- **Minimum version:** CMake 3.10
- **Language:** C++11 (upgradeable to C++14 on MSVC)
- **Version source:** Parsed from `public/common/TracyVersion.hpp`
- **Main target:** `TracyClient` (static by default, shared via `BUILD_SHARED_LIBS`)
- **Optional target:** `TracyClientF90` (Fortran bindings, via `TRACY_Fortran`)
- **Alias targets:** `Tracy::TracyClient`, `Tracy::TracyClient_Fortran`

**Key CMake options:**
| Option | Default | Description |
|--------|---------|-------------|
| `TRACY_STATIC` | ON | Build as static library |
| `TRACY_Fortran` | OFF | Build Fortran bindings |
| `TRACY_LTO` | OFF | Enable Link-Time Optimization |
| `TRACY_ENABLE` | OFF | Enable profiling (must be ON for zones to work) |
| `TRACY_ON_DEMAND` | OFF | On-demand profiling (enable/disable at runtime) |
| `TRACY_CALLSTACK` | (empty) | Override callstack depth |
| `TRACY_NO_CALLSTACK` | OFF | Disable callstack collection |
| `TRACY_NO_SAMPLING` | OFF | Disable call stack sampling |
| `TRACY_NO_CONTEXT_SWITCH` | OFF | Disable context switch capture |
| `TRACY_NO_FRAME_IMAGE` | OFF | Disable frame image/screenshot capture |
| `TRACY_NO_SYSTEM_TRACING` | OFF | Disable systrace sampling |
| `TRACY_NO_CODE_TRANSFER` | OFF | Disable source code collection |
| `TRACY_DELAYED_INIT` | OFF | Delay init until first call |
| `TRACY_MANUAL_LIFETIME` | OFF | Manual lifetime management |
| `TRACY_FIBERS` | OFF | Enable fibers support |
| `TRACY_LIBUNWIND_BACKTRACE` | OFF | Use libunwind for backtracing |
| `TRACY_DEBUGINFOD` | OFF | Enable debuginfod support |

### Meson (`meson.build`, `meson.options`)
- **Minimum version:** Meson 1.3.0
- **Target:** `tracy` library (static or shared via `default_library`)
- **Generates:** pkg-config file via `pkg.generate()`
- **Options:** Mirrors the CMake options

---

## Key Files for Quick Reference

| What you need | File location |
|---------------|---------------|
| Version number | `public/common/TracyVersion.hpp` |
| Wire protocol definition | `public/common/TracyProtocol.hpp` |
| Queue event types | `public/common/TracyQueue.hpp` |
| C++ API entry point | `public/tracy/Tracy.hpp` |
| C API entry point | `public/tracy/TracyC.h` |
| Main client source | `public/TracyClient.cpp` |
| Profiler singleton | `public/client/TracyProfiler.hpp` |
| Core data processor | `server/TracyWorker.hpp` |
| File format reader | `server/TracyFileRead.hpp` |
| File format writer | `server/TracyFileWrite.hpp` |
| Main GUI entry | `profiler/src/main.cpp` |
| Main UI view | `profiler/src/profiler/TracyView.cpp` |
| Python client bindings | `python/bindings/Module.cpp` |
| Python server bindings | `python/bindings/ServerModule.cpp` |
| User manual (Markdown) | `manual/tracy.md` |
| User manual (LaTeX) | `manual/tracy.tex` |
| Changelog | `NEWS` |
| Test suite | `test/test.cpp` |
