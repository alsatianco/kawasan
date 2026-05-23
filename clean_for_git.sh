#!/bin/bash
#
# clean_for_git.sh - Clean build artifacts and generated files before git operations
#
# This script removes all build artifacts, cached files, and other generated
# content that should not be committed to git.
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${GREEN}=== Cleaning build artifacts for git ===${NC}"
echo "Working directory: $SCRIPT_DIR"
echo ""

# Function to safely remove directory
remove_dir() {
    local dir="$1"
    local desc="$2"
    if [ -d "$dir" ]; then
        local size=$(du -sh "$dir" 2>/dev/null | cut -f1)
        echo -e "${YELLOW}Removing $desc ($size): $dir${NC}"
        rm -rf "$dir"
    fi
}

# Function to safely remove file
remove_file() {
    local file="$1"
    local desc="$2"
    if [ -f "$file" ]; then
        echo -e "${YELLOW}Removing $desc: $file${NC}"
        rm -f "$file"
    fi
}

# ============================================
# Build directories
# ============================================
echo -e "\n${GREEN}[1/7] Cleaning build directories...${NC}"

remove_dir "build" "CMake build directory"
remove_dir "cmake-build-debug" "CLion debug build"
remove_dir "cmake-build-release" "CLion release build"
remove_dir "out" "out directory"

# Remove any cmake-build-* directories
for dir in cmake-build-*; do
    if [ -d "$dir" ]; then
        remove_dir "$dir" "CMake build directory"
    fi
done

# ============================================
# vcpkg artifacts
# ============================================
echo -e "\n${GREEN}[2/7] Cleaning vcpkg artifacts...${NC}"

remove_dir "external/vcpkg" "vcpkg package manager"
remove_dir "vcpkg_installed" "vcpkg installed packages"
remove_file ".vcpkg-root" "vcpkg root marker"

# ============================================
# Python artifacts
# ============================================
echo -e "\n${GREEN}[3/7] Cleaning Python artifacts...${NC}"

remove_dir "scripts/env" "Python virtual environment"

# Remove all __pycache__ directories
find . -type d -name "__pycache__" -print0 2>/dev/null | while IFS= read -r -d '' dir; do
    echo -e "${YELLOW}Removing Python cache: $dir${NC}"
    rm -rf "$dir"
done

# Remove .pyc files
find . -name "*.pyc" -type f -delete 2>/dev/null
find . -name "*.pyo" -type f -delete 2>/dev/null

# ============================================
# macOS artifacts
# ============================================
echo -e "\n${GREEN}[4/7] Cleaning macOS artifacts...${NC}"

# Remove all .DS_Store files
find . -name ".DS_Store" -type f -print0 2>/dev/null | while IFS= read -r -d '' file; do
    echo -e "${YELLOW}Removing: $file${NC}"
    rm -f "$file"
done

# ============================================
# Compiled files and libraries
# ============================================
echo -e "\n${GREEN}[5/7] Cleaning compiled files...${NC}"

# Remove object files, libraries, and executables in source directories
# (but not in build/ which is already removed)
find . -path ./build -prune -o -path ./external -prune -o \( \
    -name "*.o" -o \
    -name "*.a" -o \
    -name "*.so" -o \
    -name "*.dylib" -o \
    -name "*.dll" -o \
    -name "*.exe" \
\) -type f -print 2>/dev/null | grep -v "^\./build" | grep -v "^\./external" | while read -r file; do
    echo -e "${YELLOW}Removing compiled file: $file${NC}"
    rm -f "$file"
done

# ============================================
# CMake generated files (outside build dir)
# ============================================
echo -e "\n${GREEN}[6/7] Cleaning CMake generated files...${NC}"

# These might exist in source dir if in-source build was done
remove_file "CMakeCache.txt" "CMake cache"
remove_file "cmake_install.cmake" "CMake install script"
remove_file "compile_commands.json" "Compilation database"
remove_file "install_manifest.txt" "Install manifest"
remove_file "Makefile" "Generated Makefile"
remove_dir "CMakeFiles" "CMake files directory"
remove_dir "Testing" "CTest directory"

# ============================================
# Coverage and profiling data
# ============================================
echo -e "\n${GREEN}[7/7] Cleaning coverage and profiling data...${NC}"

find . -name "*.gcda" -type f -delete 2>/dev/null
find . -name "*.gcno" -type f -delete 2>/dev/null
find . -name "*.gcov" -type f -delete 2>/dev/null
remove_dir "coverage" "Coverage reports"

# ============================================
# Logs and temporary data
# ============================================
echo -e "\n${GREEN}Cleaning logs and temporary data...${NC}"

find . -name "*.log" -type f -print0 2>/dev/null | while IFS= read -r -d '' file; do
    # Don't delete logs in docs or config examples
    if [[ "$file" != *"/docs/"* ]] && [[ "$file" != *"/config/"* ]]; then
        echo -e "${YELLOW}Removing log: $file${NC}"
        rm -f "$file"
    fi
done

remove_dir "data" "Runtime data directory"
remove_dir "logs" "Logs directory"

# ============================================
# Editor backup files
# ============================================
echo -e "\n${GREEN}Cleaning editor backup files...${NC}"

find . -name "*.swp" -type f -delete 2>/dev/null
find . -name "*.swo" -type f -delete 2>/dev/null
find . -name "*~" -type f -delete 2>/dev/null

# ============================================
# Summary
# ============================================
echo ""
echo -e "${GREEN}=== Cleaning complete ===${NC}"
echo ""

# Show what would be committed
echo -e "${GREEN}Git status after cleaning:${NC}"
git status --short 2>/dev/null || true
