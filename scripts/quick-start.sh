#!/bin/bash
# Quick start script for Kawasan

set -e

echo "======================================"
echo "  Kawasan Quick Start"
echo "======================================"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PROJECT_ROOT=$(pwd -P)
DETECTED_VCPKG=""
MACOS_LLVM_LIBCXX_FLAGS=""

# Check if running from correct directory
if [ ! -f "CMakeLists.txt" ]; then
    echo -e "${RED}Error: Please run this script from the kawasan root directory${NC}"
    exit 1
fi

# Step 1: Check dependencies
echo -e "${YELLOW}[1/5] Checking dependencies...${NC}"

check_command() {
    if command -v $1 &> /dev/null; then
        echo -e "${GREEN}✓${NC} $1 found"
        return 0
    else
        echo -e "${RED}✗${NC} $1 not found"
        return 1
    fi
}

detect_vcpkg() {
    if [ -n "$VCPKG_ROOT" ] && [ -f "$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" ]; then
        DETECTED_VCPKG=$(cd "$VCPKG_ROOT" && pwd -P)
        return 0
    fi

    local repo_vcpkg="${PROJECT_ROOT}/external/vcpkg"
    if [ -f "$repo_vcpkg/scripts/buildsystems/vcpkg.cmake" ]; then
        DETECTED_VCPKG="$repo_vcpkg"
        return 0
    fi

    if command -v vcpkg >/dev/null 2>&1; then
        local vcpkg_bin
        vcpkg_bin=$(command -v vcpkg)
        local candidate_root
        candidate_root=$(dirname "$(dirname "$vcpkg_bin")")
        if [ -f "$candidate_root/scripts/buildsystems/vcpkg.cmake" ]; then
            DETECTED_VCPKG="$candidate_root"
            return 0
        fi
    fi

    return 1
}

bootstrap_vcpkg() {
    local target_dir="${PROJECT_ROOT}/external/vcpkg"

    if [ ! -d "$target_dir" ]; then
        echo "Cloning vcpkg into ${target_dir}..."
        mkdir -p "$(dirname "$target_dir")"
        if ! command -v git >/dev/null 2>&1; then
            echo -e "${RED}git is required to download vcpkg automatically.${NC}"
            return 1
        fi
        if ! git clone https://github.com/microsoft/vcpkg.git "$target_dir"; then
            echo -e "${RED}Failed to clone vcpkg repository.${NC}"
            return 1
        fi
    else
        echo "vcpkg checkout found at ${target_dir}"
    fi

    if [ ! -x "$target_dir/vcpkg" ]; then
        echo "Bootstrapping vcpkg..."
        if ! "$target_dir/bootstrap-vcpkg.sh"; then
            echo -e "${RED}vcpkg bootstrap script failed.${NC}"
            return 1
        fi
    fi

    DETECTED_VCPKG="$target_dir"
    return 0
}

ensure_vcpkg_full_clone() {
    local repo_path="$1"
    if [ ! -d "$repo_path/.git" ]; then
        return 0
    fi
    if ! command -v git >/dev/null 2>&1; then
        return 0
    fi
    local is_shallow
    is_shallow=$(git -C "$repo_path" rev-parse --is-shallow-repository 2>/dev/null || echo "false")
    if [ "$is_shallow" = "true" ]; then
        echo "Fetching full vcpkg history to satisfy versioned ports..."
        if ! git -C "$repo_path" fetch --tags --prune --unshallow; then
            echo -e "${RED}Failed to fetch full vcpkg git history.${NC}"
            return 1
        fi
    fi
    return 0
}

reset_cmake_cache() {
    rm -f CMakeCache.txt
    rm -rf CMakeFiles
}

setup_macos_libcxx_overrides() {
    if [ "$(uname -s)" != "Darwin" ]; then
        return
    fi

    local clang_path
    clang_path=$(command -v clang++ 2>/dev/null || true)
    if [ -z "$clang_path" ]; then
        return
    fi

    case "$clang_path" in
        */opt/homebrew/*/llvm/bin/clang++)
            ;;
        *)
            return
            ;;
    esac

    local llvm_prefix
    llvm_prefix=$(cd "$(dirname "$clang_path")/.." && pwd -P)
    local libcxx_dir="${llvm_prefix}/lib/c++"

    if [ ! -d "$libcxx_dir" ]; then
        return
    fi

    MACOS_LLVM_LIBCXX_FLAGS="-L${libcxx_dir} -Wl,-rpath,${libcxx_dir}"
    export VCPKG_LINKER_FLAGS="${MACOS_LLVM_LIBCXX_FLAGS} ${VCPKG_LINKER_FLAGS:-}"
    export LDFLAGS="${MACOS_LLVM_LIBCXX_FLAGS} ${LDFLAGS:-}"

    echo -e "${YELLOW}[macOS] Detected Homebrew LLVM; forcing libc++ runtime from ${libcxx_dir}${NC}"
    echo -e "${YELLOW}[macOS] Set KAWASAN_DISABLE_LIBCXX_OVERRIDE=1 to skip this adjustment if undesired.${NC}"
}

MISSING_DEPS=0
check_command cmake || MISSING_DEPS=1
check_command g++ || check_command clang++ || MISSING_DEPS=1
check_command pkg-config || MISSING_DEPS=1
check_command make || check_command ninja || MISSING_DEPS=1

if [ $MISSING_DEPS -eq 1 ]; then
    echo ""
    echo -e "${RED}Missing dependencies. Please install them first.${NC}"
    echo "See docs/GETTING_STARTED.md for installation instructions."
    exit 1
fi

# Step 2: Build
echo ""
echo -e "${YELLOW}[2/5] Building Kawasan...${NC}"

BUILD_TYPE=${KAWASAN_BUILD_TYPE:-RelWithDebInfo}
declare -a CMAKE_ARGS
CMAKE_ARGS=(-DCMAKE_BUILD_TYPE="${BUILD_TYPE}")
EXPECTED_TOOLCHAIN=""

if [ "${KAWASAN_DISABLE_LIBCXX_OVERRIDE:-0}" != "1" ]; then
    setup_macos_libcxx_overrides
fi

if [ -n "$MACOS_LLVM_LIBCXX_FLAGS" ]; then
    CMAKE_ARGS+=(
        "-DCMAKE_EXE_LINKER_FLAGS=${MACOS_LLVM_LIBCXX_FLAGS}"
        "-DCMAKE_SHARED_LINKER_FLAGS=${MACOS_LLVM_LIBCXX_FLAGS}"
        "-DCMAKE_MODULE_LINKER_FLAGS=${MACOS_LLVM_LIBCXX_FLAGS}"
    )
fi

USE_VCPKG=1
if [ "${KAWASAN_SKIP_VCPKG:-0}" = "1" ]; then
    USE_VCPKG=0
    echo -e "${YELLOW}Skipping automatic vcpkg setup (KAWASAN_SKIP_VCPKG=1).${NC}"
    echo "Make sure Boost, RocksDB, spdlog, nlohmann-json, GTest, and compression libraries are installed via your system package manager."
fi

VCPKG_ROOT_PATH=""
if [ $USE_VCPKG -eq 1 ]; then
    echo "Resolving dependencies with vcpkg..."
    if detect_vcpkg; then
        VCPKG_ROOT_PATH="$DETECTED_VCPKG"
    else
        echo "vcpkg not found. Bootstrapping a local copy (set KAWASAN_SKIP_VCPKG=1 to opt out)..."
        if bootstrap_vcpkg; then
            VCPKG_ROOT_PATH="$DETECTED_VCPKG"
        fi
    fi

    if [ -n "$VCPKG_ROOT_PATH" ]; then
        if ! ensure_vcpkg_full_clone "$VCPKG_ROOT_PATH"; then
            VCPKG_ROOT_PATH=""
        fi
    fi

    if [ -n "$VCPKG_ROOT_PATH" ]; then
        export VCPKG_ROOT="$VCPKG_ROOT_PATH"
        EXPECTED_TOOLCHAIN="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
        CMAKE_ARGS+=(
            -DCMAKE_TOOLCHAIN_FILE="$EXPECTED_TOOLCHAIN"
            -DVCPKG_MANIFEST_MODE=ON
        )
        echo -e "${GREEN}✓${NC} Using vcpkg toolchain at $VCPKG_ROOT"
    else
        echo -e "${YELLOW}!${NC} Proceeding without vcpkg; system packages must satisfy all dependencies."
    fi
fi

if [ ! -d "build" ]; then
    mkdir build
fi

cd build

NEEDS_CONFIGURE=0

if [ ! -f "CMakeCache.txt" ]; then
    NEEDS_CONFIGURE=1
else
    GENERATOR=$(sed -n 's/^CMAKE_GENERATOR:INTERNAL=//p' CMakeCache.txt | head -n 1)
    if [[ "$GENERATOR" == *"Ninja"* ]]; then
        [ -f "build.ninja" ] || NEEDS_CONFIGURE=1
    else
        [ -f "Makefile" ] || NEEDS_CONFIGURE=1
    fi
fi

if [ -f "CMakeCache.txt" ] && [ -n "$EXPECTED_TOOLCHAIN" ]; then
    CURRENT_TOOLCHAIN=$(sed -n 's/^CMAKE_TOOLCHAIN_FILE:.*=//p' CMakeCache.txt | head -n 1)
    if [ "$CURRENT_TOOLCHAIN" != "$EXPECTED_TOOLCHAIN" ]; then
        echo "Toolchain mismatch detected, resetting CMake cache..."
        NEEDS_CONFIGURE=1
        reset_cmake_cache
    fi
fi

if [ $NEEDS_CONFIGURE -eq 0 ] && [ -f "CMakeCache.txt" ]; then
    CACHE_BUILD_TOOL=$(sed -n 's/^CMAKE_MAKE_PROGRAM:[^=]*=//p' CMakeCache.txt | head -n 1)
    if [ -z "$CACHE_BUILD_TOOL" ]; then
        echo "Existing CMake cache is incomplete (missing build tool); reconfiguring..."
        NEEDS_CONFIGURE=1
        reset_cmake_cache
    elif [ ! -x "$CACHE_BUILD_TOOL" ]; then
        echo "Configured build tool '$CACHE_BUILD_TOOL' is unavailable; reconfiguring..."
        NEEDS_CONFIGURE=1
        reset_cmake_cache
    fi
fi

if [ $NEEDS_CONFIGURE -eq 1 ]; then
    echo "Configuring build..."
    cmake .. "${CMAKE_ARGS[@]}" || {
        echo -e "${RED}CMake configuration failed${NC}"
        exit 1
    }
else
    echo "Build directory exists, using existing configuration..."
fi

echo "Compiling..."
cmake --build . -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4) || {
    echo -e "${RED}Build failed${NC}"
    exit 1
}

cd ..

echo -e "${GREEN}✓ Build completed successfully${NC}"

# Step 3: Create config
echo ""
echo -e "${YELLOW}[3/5] Creating configuration...${NC}"

CONFIG_PROFILE=${KAWASAN_PROFILE:-local}
LISTEN_HOST=${KAWASAN_LISTEN_HOST:-localhost}
LISTEN_PORT=${KAWASAN_PORT:-9092}
LOG_DIR=${KAWASAN_LOG_DIR:-/tmp/kawasan-logs}
CONFIG_PATH=${KAWASAN_CONFIG_PATH:-/tmp/kawasan-server.properties}

# For Docker-based tools like Kafka UI, advertise a host that containers can reach.
if [ "$CONFIG_PROFILE" = "kafka-ui" ]; then
    LISTEN_HOST=${KAWASAN_LISTEN_HOST:-0.0.0.0}
    DEFAULT_ADVERTISED_HOST="host.docker.internal"
else
    DEFAULT_ADVERTISED_HOST=$LISTEN_HOST
fi

ADVERTISED_HOST=${KAWASAN_ADVERTISED_HOST:-$DEFAULT_ADVERTISED_HOST}
ADVERTISED_PORT=${KAWASAN_ADVERTISED_PORT:-$LISTEN_PORT}

mkdir -p "$LOG_DIR"
mkdir -p "$(dirname "$CONFIG_PATH")"

cat > "$CONFIG_PATH" << EOF
{
  "broker.id": 0,
  "host": "${LISTEN_HOST}",
  "advertised.host": "${ADVERTISED_HOST}",
  "port": ${LISTEN_PORT},
  "advertised.port": ${ADVERTISED_PORT},
  "log.dirs": "${LOG_DIR}"
}
EOF

echo "  Profile        : ${CONFIG_PROFILE}"
echo "  Listen address : ${LISTEN_HOST}:${LISTEN_PORT}"
echo "  Advertised     : ${ADVERTISED_HOST}:${ADVERTISED_PORT}"
echo "  Log directory  : ${LOG_DIR}"
echo -e "${GREEN}✓ Configuration created at ${CONFIG_PATH}${NC}"

# Step 4: Start broker
echo ""
echo -e "${YELLOW}[4/5] Starting broker...${NC}"

# Create log directory
mkdir -p /tmp/kawasan-broker-logs

# Start broker with output redirected
./build/tools/kawasan-broker \
    --config "${CONFIG_PATH}" \
    --log-level info \
    >> /tmp/kawasan-broker-logs/broker.log 2>&1 &

BROKER_PID=$!
echo $BROKER_PID > /tmp/kawasan-broker.pid

echo -e "${GREEN}✓ Broker started (PID: $BROKER_PID)${NC}"
echo "  Logs: /tmp/kawasan-broker-logs/broker.log"

# Step 5: Wait and verify
echo ""
echo -e "${YELLOW}[5/5] Verifying broker...${NC}"

sleep 3

if kill -0 $BROKER_PID 2>/dev/null; then
    echo -e "${GREEN}✓ Broker is running${NC}"
else
    echo -e "${RED}✗ Broker failed to start. Check logs:${NC}"
    echo "  tail -f /tmp/kawasan-logs/*.log"
    exit 1
fi

# Success message
echo ""
echo -e "${GREEN}======================================"
echo "  Kawasan is ready!"
echo -e "======================================${NC}"
echo ""
echo "Broker is running on localhost:9092"
echo ""
echo "Next steps:"
echo "  1. Use any Kafka client to connect"
echo "  2. Run examples: ./build/examples/simple_producer"
echo "  3. View logs: tail -f /tmp/kawasan-logs/*.log"
echo ""
echo "To stop the broker:"
echo "  kill \$(cat /tmp/kawasan-broker.pid)"
echo ""
echo "For more information, see docs/GETTING_STARTED.md"
