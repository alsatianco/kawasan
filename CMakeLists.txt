cmake_minimum_required(VERSION 3.20)
project(kawasan VERSION 0.1.0 LANGUAGES CXX)

# Prefer legacy FindBoost module until the project migrates to Boost's CMake config packages
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 OLD)
endif()

# C++20 standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Export compile commands for IDE integration
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Build options
option(KAWASAN_BUILD_TESTS "Build tests" ON)
option(KAWASAN_BUILD_TOOLS "Build CLI tools" ON)
option(KAWASAN_BUILD_EXAMPLES "Build examples" ON)
option(KAWASAN_ENABLE_ASAN "Enable Address Sanitizer" OFF)
option(KAWASAN_ENABLE_TSAN "Enable Thread Sanitizer" OFF)
# Phase EX-2 (§6.2): libFuzzer drivers. Requires clang.
option(KAWASAN_BUILD_FUZZ "Build libFuzzer harnesses (clang required)" OFF)

# Add cmake module path
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Prefer the FindBoost module over Boost's config packages only when not using the vcpkg toolchain
if(NOT (DEFINED CMAKE_TOOLCHAIN_FILE AND CMAKE_TOOLCHAIN_FILE MATCHES "vcpkg\\.cmake$"))
    set(Boost_NO_BOOST_CMAKE ON)
endif()

# Compiler warnings
if(MSVC)
    add_compile_options(/W4 /WX)
else()
    add_compile_options(-Wall -Wextra -Wpedantic -Werror)
endif()

# Sanitizers
if(KAWASAN_ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

if(KAWASAN_ENABLE_TSAN)
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()

# P13: instrument the WHOLE build with libFuzzer coverage (no main-link) so
# fuzz drivers get coverage feedback from the library code under test, not
# just from the driver TU. The drivers themselves add -fsanitize=fuzzer
# (tests/fuzz/CMakeLists.txt) to link libFuzzer's main.
if(KAWASAN_BUILD_FUZZ)
    add_compile_options(-fsanitize=fuzzer-no-link -fno-omit-frame-pointer)
endif()

# Find dependencies
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
# Boost 1.89+ doesn't have a separate system library, so we try without it first
find_package(Boost 1.75 COMPONENTS program_options)
if(NOT Boost_FOUND)
    # Try with system component for older Boost versions
    find_package(Boost 1.75 REQUIRED COMPONENTS system program_options)
endif()
find_package(RocksDB REQUIRED)
find_package(spdlog REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Snappy CONFIG REQUIRED)

# Try to find lz4 with CONFIG first, fallback to finding the library manually
find_package(lz4 CONFIG QUIET)
if(NOT lz4_FOUND)
    find_path(LZ4_INCLUDE_DIR NAMES lz4.h PATHS /opt/homebrew/include /usr/local/include)
    find_library(LZ4_LIBRARY NAMES lz4 PATHS /opt/homebrew/lib /usr/local/lib)
    if(LZ4_INCLUDE_DIR AND LZ4_LIBRARY)
        set(lz4_FOUND TRUE)
        add_library(lz4::lz4 UNKNOWN IMPORTED)
        set_target_properties(lz4::lz4 PROPERTIES
            IMPORTED_LOCATION "${LZ4_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}")
        message(STATUS "Found lz4: ${LZ4_LIBRARY}")
    else()
        message(FATAL_ERROR "lz4 not found")
    endif()
endif()

find_package(zstd CONFIG REQUIRED)

if(KAWASAN_BUILD_TESTS)
    find_package(GTest REQUIRED)
    enable_testing()
endif()

# Include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# Subdirectories
add_subdirectory(src)

if(KAWASAN_BUILD_TESTS)
    add_subdirectory(tests)
endif()

if(KAWASAN_BUILD_TOOLS)
    add_subdirectory(tools)
endif()

if(KAWASAN_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Install targets
install(DIRECTORY include/kawasan
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
)

# Package configuration
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/kawasanConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/kawasanConfigVersion.cmake"
    DESTINATION lib/cmake/kawasan
)
