@echo off
REM start_bridge.bat — Launch the BAR + Tracy Bridge MCP server
REM
REM Usage:
REM   start_bridge.bat                  REM stdio mode (default)
REM   start_bridge.bat --sse            REM SSE mode
REM   start_bridge.bat --sse --port 47381
REM
REM Environment:
REM   BAR_MCP_HOST      — BAR MCP hostname     (default: 127.0.0.1)
REM   BAR_MCP_PORT      — BAR MCP port         (default: 23452)
REM   TRACY_MCP_HOST    — Tracy MCP hostname   (default: 127.0.0.1)
REM   TRACY_MCP_PORT    — Tracy MCP port       (default: 47380)
REM   TRACY_ENGINE_HOST — Tracy engine host    (default: 127.0.0.1)
REM   TRACY_ENGINE_PORT — Tracy engine port    (default: 8086)
REM   TRACY_ENGINE_ALIAS — Tracy instance alias (default: live_engine)
REM   BRIDGE_LOG_LEVEL  — Log level            (default: INFO)
REM
REM This script:
REM   1. Sets PYTHONPATH for TracyServerBindings
REM   2. Sources local overrides (start_bridge.local.bat)
REM   3. Starts the bridge (Tracy MCP auto-start is handled inside the bridge)

setlocal EnableDelayedExpansion

REM Resolve script directory
set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
set "BRIDGE_SCRIPT=%SCRIPT_DIR%\bar_tracy_bridge.py"

REM Set PYTHONPATH to include TracyServerBindings build output.
REM Adjust the Release/Debug suffix to match your CMake build configuration.
if defined PYTHONPATH (
    set "PYTHONPATH=!PYTHONPATH!;%SCRIPT_DIR%\..\..\build\python\Release"
) else (
    set "PYTHONPATH=%SCRIPT_DIR%\..\..\build\python\Release"
)

REM Default env vars if not set
if not defined BAR_MCP_HOST set "BAR_MCP_HOST=127.0.0.1"
if not defined BAR_MCP_PORT set "BAR_MCP_PORT=23452"
if not defined TRACY_MCP_HOST set "TRACY_MCP_HOST=127.0.0.1"
if not defined TRACY_MCP_PORT set "TRACY_MCP_PORT=47380"
if not defined TRACY_ENGINE_HOST set "TRACY_ENGINE_HOST=127.0.0.1"
if not defined TRACY_ENGINE_PORT set "TRACY_ENGINE_PORT=8086"
if not defined TRACY_ENGINE_ALIAS set "TRACY_ENGINE_ALIAS=live_engine"
if not defined BRIDGE_LOG_LEVEL set "BRIDGE_LOG_LEVEL=INFO"

REM Machine-local overrides (not committed). Create start_bridge.local.bat next to
REM this file to set BAR_MCP_PORT, TRACY_MCP_PORT, BRIDGE_LOG_LEVEL, etc:
REM   set BAR_MCP_PORT=23452
REM   set TRACY_MCP_PORT=47380
REM   set TRACY_ENGINE_PORT=8086
REM   set BRIDGE_LOG_LEVEL=DEBUG
if exist "%SCRIPT_DIR%\start_bridge.local.bat" (
    call "%SCRIPT_DIR%\start_bridge.local.bat"
)

REM Determine Python executable
if not defined PYTHON set "PYTHON=python"

REM Parse arguments — build bridge args
set "BRIDGE_ARGS="
set "TRANSPORT=stdio"

:argloop
if "%~1"=="" goto :afterargs
if /i "%~1"=="--sse" (
    set "TRANSPORT=sse"
    set "BRIDGE_ARGS=!BRIDGE_ARGS! --transport sse"
) else if /i "%~1"=="--stdio" (
    set "TRANSPORT=stdio"
    set "BRIDGE_ARGS=!BRIDGE_ARGS! --transport stdio"
) else (
    set "BRIDGE_ARGS=!BRIDGE_ARGS! %~1"
)
shift
goto :argloop

:afterargs

echo [BRIDGE] Starting BAR + Tracy Bridge
echo [BRIDGE] BAR target:   %BAR_MCP_HOST%:%BAR_MCP_PORT%
echo [BRIDGE] Tracy target: %TRACY_MCP_HOST%:%TRACY_MCP_PORT%
echo [BRIDGE] Engine Tracy: %TRACY_ENGINE_HOST%:%TRACY_ENGINE_PORT% as %TRACY_ENGINE_ALIAS%
echo [BRIDGE] Transport:    %TRANSPORT%
echo [BRIDGE] Auto-start Tracy MCP: handled inside bridge
echo [BRIDGE] Command: %PYTHON% %BRIDGE_SCRIPT% %BRIDGE_ARGS%

REM Start the bridge server
REM The bridge handles Tracy MCP auto-start, auto-connect, and BAR connection internally
%PYTHON% "%BRIDGE_SCRIPT%" %BRIDGE_ARGS%
