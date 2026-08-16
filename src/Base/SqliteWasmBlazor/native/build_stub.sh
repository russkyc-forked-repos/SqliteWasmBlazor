#!/bin/bash
# Build minimal SQLite stub (fast - compiles in <1 second)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/lib"

echo "========================================="
echo "Minimal SQLite Stub Build (Fast)"
echo "========================================="
echo ""

# Toolchain lookup order:
#   1. emcc already in PATH (e.g., from GitHub Actions)
#   2. standalone EMSDK via EMSDK_PATH
#   3. the .NET wasm-tools workload's Emscripten pack (same toolchain the
#      Blazor WASM native relink uses, so stub and app link bit-compatibly)
if ! command -v emcc &> /dev/null; then
    if [ -n "${EMSDK_PATH:-}" ] && [ -f "${EMSDK_PATH}/emsdk_env.sh" ]; then
        source "${EMSDK_PATH}/emsdk_env.sh" > /dev/null 2>&1
    else
        DOTNET_ROOT="${DOTNET_ROOT:-/usr/local/share/dotnet}"
        EMSDK_PACK=$(ls -d "${DOTNET_ROOT}"/packs/Microsoft.NET.Runtime.Emscripten.*.Sdk.*/*/tools 2>/dev/null | sort -V | tail -1)
        NODE_PACK=$(ls -d "${DOTNET_ROOT}"/packs/Microsoft.NET.Runtime.Emscripten.*.Node.*/*/tools 2>/dev/null | sort -V | tail -1)
        if [ -n "${EMSDK_PACK}" ] && [ -x "${EMSDK_PACK}/emscripten/emcc" ] && [ -x "${NODE_PACK}/bin/node" ]; then
            export DOTNET_EMSCRIPTEN_LLVM_ROOT="${EMSDK_PACK}/bin"
            export DOTNET_EMSCRIPTEN_BINARYEN_ROOT="${EMSDK_PACK}"
            export DOTNET_EMSCRIPTEN_NODE_JS="${NODE_PACK}/bin/node"
            export EM_CACHE="${TMPDIR:-/tmp}/emcache-sqlite-stub"
            export FROZEN_CACHE=
            export PATH="${EMSDK_PACK}/emscripten:${EMSDK_PACK}/bin:${PATH}"
        else
            echo "ERROR: Emscripten not found. Install emsdk (set EMSDK_PATH) or the .NET wasm-tools workload."
            exit 1
        fi
    fi
fi

# Create output directory
mkdir -p "${OUTPUT_DIR}"

# Compile stub (fast!)
echo "Compiling sqlite3_stub.c..."
emcc -O3 \
    -c "${SCRIPT_DIR}/sqlite3_stub.c" \
    -o "${OUTPUT_DIR}/sqlite3_stub.o"

# Create static library
echo "Creating library..."
emar rcs "${OUTPUT_DIR}/e_sqlite3.a" "${OUTPUT_DIR}/sqlite3_stub.o"

# Check result
if [ -f "${OUTPUT_DIR}/e_sqlite3.a" ]; then
    SIZE=$(du -h "${OUTPUT_DIR}/e_sqlite3.a" | cut -f1)
    echo ""
    echo "✓ Build successful!"
    echo ""
    echo "Output: ${OUTPUT_DIR}/e_sqlite3.a"
    echo "Size:   ${SIZE}"
    echo ""
    echo "This stub provides native symbols for P/Invoke."
    echo "All database operations go through JS worker bridge."
    echo ""
else
    echo "ERROR: Build failed"
    exit 1
fi
