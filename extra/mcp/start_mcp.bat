@echo off
REM Start the Tracy MCP server.
REM
REM Set PYTHONPATH to the directory containing TracyServerBindings.so/.pyd.
REM Adjust the Release/Debug suffix to match your CMake build configuration.

setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

REM Set PYTHONPATH to include TracyServerBindings build output.
if defined PYTHONPATH (
    set "PYTHONPATH=!PYTHONPATH!;%SCRIPT_DIR%\..\..\build\python\Release"
) else (
    set "PYTHONPATH=%SCRIPT_DIR%\..\..\build\python\Release"
)

REM Machine-local overrides (not committed). Create start_mcp.local.bat next to
REM this file to set TRACY_CAPTURES_DIR, TRACY_MCP_PORT, or any other env var:
REM   set TRACY_CAPTURES_DIR=C:\path\to\captures
REM   set TRACY_MCP_PORT=47380
if exist "%SCRIPT_DIR%\start_mcp.local.bat" (
    call "%SCRIPT_DIR%\start_mcp.local.bat"
)

python "%SCRIPT_DIR%\tracy_mcp.py" %*
