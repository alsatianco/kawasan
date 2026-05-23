# Test configuration
include_directories(${CMAKE_SOURCE_DIR}/include)

# Unit tests subdirectory
add_subdirectory(unit)

# Integration tests
add_subdirectory(integration)

# Benchmark tests
add_subdirectory(benchmark)

# Phase EX-2 (§6.2): fuzz harness — only enabled with -DKAWASAN_BUILD_FUZZ=ON
if(KAWASAN_BUILD_FUZZ)
    add_subdirectory(fuzz)
endif()
