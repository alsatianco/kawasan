add_library(kawasan_streams OBJECT
    topology.cpp
    streams_builder.cpp
    stream_task.cpp
    kawasan_streams.cpp
    state_store.cpp
)

target_include_directories(kawasan_streams PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kawasan_streams PUBLIC
    kawasan_common
    kawasan_client
    spdlog::spdlog
    RocksDB::RocksDB
)

target_compile_features(kawasan_streams PUBLIC cxx_std_20)

