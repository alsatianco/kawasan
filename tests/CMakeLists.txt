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

# M9 checker fixtures are dependency-free and must reject corrupt histories.
find_package(Python3 COMPONENTS Interpreter REQUIRED)
add_test(NAME ConsistencyCheckerTest
    COMMAND ${Python3_EXECUTABLE} -m unittest discover
            -s ${CMAKE_SOURCE_DIR}/scripts/tests -p test_consistency_checker.py)
add_test(NAME NemesisTest
    COMMAND ${Python3_EXECUTABLE} -m unittest discover
            -s ${CMAKE_SOURCE_DIR}/scripts/tests -p test_nemesis.py)

# CM-1 result gate must reject missing cases, skips, and stale allowlists.
add_test(NAME ClientMatrixResultTest
    COMMAND ${Python3_EXECUTABLE} -m unittest discover
            -s ${CMAKE_SOURCE_DIR}/tests/clients -p test_results.py)
