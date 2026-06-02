#!/usr/bin/env bash
# start_bridge.sh — Launch the BAR + Tracy Bridge MCP server
#
# Usage:
#   ./start_bridge.sh                  # stdio mode (default)
#   ./start_bridge.sh --sse            # SSE mode
#   ./start_bridge.sh --sse --port 47381
#
# Environment:
#   BAR_MCP_HOST      — BAR MCP hostname     (default: 127.0.0.1)
#   BAR_MCP_PORT      — BAR MCP port         (default: 23452)
#   TRACY_MCP_HOST    — Tracy MCP hostname   (default: 127.0.0.1)
#   TRACY_MCP_PORT    — Tracy MCP port       (default: 47380)
#   BRIDGE_LOG_LEVEL  — Log level            (default: INFO)
#
# This script:
#   1. Sets PYTHONPATH for TracyServerBindings
#   2. Sources local overrides (start_bridge.local.sh)
#   3. Starts the bridge (Tracy MCP auto-start is handled inside the bridge)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRIDGE_SCRIPT="$SCRIPT_DIR/bar_tracy_bridge.py"

# Set PYTHONPATH to include TracyServerBindings build output.
# Adjust the Release/Debug suffix to match your CMake build configuration.
PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}$SCRIPT_DIR/../../build/python/Release"
export PYTHONPATH

# Default env vars if not set
export BAR_MCP_HOST="${BAR_MCP_HOST:-127.0.0.1}"
export BAR_MCP_PORT="${BAR_MCP_PORT:-23452}"
export TRACY_MCP_HOST="${TRACY_MCP_HOST:-127.0.0.1}"
export TRACY_MCP_PORT="${TRACY_MCP_PORT:-47380}"
export BRIDGE_LOG_LEVEL="${BRIDGE_LOG_LEVEL:-INFO}"

# Machine-local overrides (not committed). Create start_bridge.local.sh next to
# this file to set BAR_MCP_PORT, TRACY_MCP_PORT, BRIDGE_LOG_LEVEL, etc:
#   export BAR_MCP_PORT=23452
#   export TRACY_MCP_PORT=47380
#   export BRIDGE_LOG_LEVEL=DEBUG
if [ -f "$SCRIPT_DIR/start_bridge.local.sh" ]; then
    . "$SCRIPT_DIR/start_bridge.local.sh"
fi

PYTHON="${PYTHON:-python3}"

# Parse arguments — pass everything through to the bridge script
BRIDGE_ARGS=()
TRANSPORT="stdio"

for arg in "$@"; do
    case "$arg" in
        --sse)
            TRANSPORT="sse"
            BRIDGE_ARGS+=("--transport" "sse")
            ;;
        --stdio)
            TRANSPORT="stdio"
            BRIDGE_ARGS+=("--transport" "stdio")
            ;;
        *)
            BRIDGE_ARGS+=("$arg")
            ;;
    esac
done

echo "[BRIDGE] Starting BAR + Tracy Bridge"
echo "[BRIDGE] BAR target:   $BAR_MCP_HOST:$BAR_MCP_PORT"
echo "[BRIDGE] Tracy target: $TRACY_MCP_HOST:$TRACY_MCP_PORT"
echo "[BRIDGE] Transport:    $TRANSPORT"
echo "[BRIDGE] Auto-start Tracy MCP: handled inside bridge"
echo "[BRIDGE] Command: $PYTHON $BRIDGE_SCRIPT ${BRIDGE_ARGS[*]:-}"

# Start the bridge server
# The bridge handles Tracy MCP auto-start, auto-connect, and BAR connection internally
exec "$PYTHON" "$BRIDGE_SCRIPT" "${BRIDGE_ARGS[@]:-}"
