# Phase EX-2 (§6.2): wire-protocol fuzz harness.
#
# Builds only when -DKAWASAN_BUILD_FUZZ=ON is passed. Each fuzzer is a
# small driver that pipes bytes into a high-traffic decoder and asserts
# it doesn't crash or leak (under ASan).

if(NOT KAWASAN_BUILD_FUZZ)
    return()
endif()

# libFuzzer is bundled with clang. Use the -fsanitize=fuzzer,address
# instrumentation. (We don't enforce the compiler at configure time —
# the user has to set CC=clang CXX=clang++ themselves.)
set(FUZZ_FLAGS "-fsanitize=fuzzer,address -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${FUZZ_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${FUZZ_FLAGS}")

add_executable(fuzz_request_header fuzz_request_header.cpp)
target_link_libraries(fuzz_request_header PRIVATE kawasan)

add_executable(fuzz_record_batch fuzz_record_batch.cpp)
target_link_libraries(fuzz_record_batch PRIVATE kawasan)

# P13: request-body parsers + codec frames — every externally-reachable
# decoder gets a driver.
add_executable(fuzz_produce_request fuzz_produce_request.cpp)
target_link_libraries(fuzz_produce_request PRIVATE kawasan)

add_executable(fuzz_fetch_request fuzz_fetch_request.cpp)
target_link_libraries(fuzz_fetch_request PRIVATE kawasan)

add_executable(fuzz_metadata_request fuzz_metadata_request.cpp)
target_link_libraries(fuzz_metadata_request PRIVATE kawasan)

add_executable(fuzz_compression fuzz_compression.cpp)
target_link_libraries(fuzz_compression PRIVATE kawasan)
