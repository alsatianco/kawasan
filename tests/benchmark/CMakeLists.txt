add_executable(replication_benchmark replication_benchmark.cpp)
target_link_libraries(replication_benchmark PRIVATE kawasan)

add_executable(throughput_benchmark throughput_benchmark.cpp)
target_link_libraries(throughput_benchmark PRIVATE kawasan)

add_executable(load_test load_test.cpp)
target_link_libraries(load_test PRIVATE kawasan)

# Streams benchmark (Week 22)
add_executable(streams_benchmark streams_benchmark.cpp)
target_link_libraries(streams_benchmark PRIVATE kawasan)

# Benchmarks are not run as part of regular tests
# Run manually with:
#   ./tests/benchmark/replication_benchmark
#   ./tests/benchmark/throughput_benchmark
#   ./tests/benchmark/load_test
#   ./tests/benchmark/streams_benchmark
