Here's the complete summary:

## Build Tracy Python Bindings on WSL (Ubuntu 24.04)

### 1. Install system dependencies
```bash
sudo apt install python3.12-venv pkg-config libdbus-1-dev libssl-dev build-essential cmake ninja-build
```

### 2. Create & activate a virtual environment
```bash
python3 -m venv ~/tracy-venv
source ~/tracy-venv/bin/activate
```

### 3. Install Python build dependencies
```bash
pip install scikit-build-core
```

### 4. Apply fixes to source files

**CMakeLists.txt** — Add `C` language support (vendored deps need it):
```cmake
project(Tracy LANGUAGES C CXX VERSION ${TRACY_VERSION_STRING})
```

**pyproject.toml** — Fix install directories for vendored libs:
```toml
[tool.scikit-build.cmake.define]
TRACY_CLIENT_PYTHON = "ON"
TRACY_STATIC = "OFF"
CMAKE_INSTALL_BINDIR = "."
CMAKE_INSTALL_LIBDIR = "lib"
CMAKE_INSTALL_INCLUDEDIR = "include"
```

### 5. Build & install
```bash
cd /mnt/n/github/tracy/python
pip install .
```

This builds both `TracyClientBindings` (client instrumentation) and `TracyServerBindings` (trace file analysis) as Python extension modules.

## Build Tracy Python Bindings on Windows

### 1. Install Build Tools
- **Visual Studio 2022**: Install the "Desktop development with C++" workload.
- **CMake**: Download and install, ensuring it's added to your System PATH.
- **Python 3.10+**: Installed on Windows.

### 2. Prepare the Environment
Open a PowerShell terminal (or "Developer PowerShell for VS 2022"):
```powershell
cd n:\github\tracy\python
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 3. Install Build Dependencies
```powershell
pip install scikit-build-core
```

### 4. Build & Install
```powershell
pip install .
```
The `.pyd` extension modules are installed into your virtual environment's `site-packages` folder (e.g., `venv\Lib\site-packages\`).

### 5. Using with Tracy MCP
With the virtual environment active, you can run the server:
```powershell
cd n:\github\tracy\extra\mcp
python tracy_mcp.py
```
