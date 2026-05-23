# Common library
add_subdirectory(common)

# Protocol library
add_subdirectory(protocol)

# Storage library
add_subdirectory(storage)

# Raft consensus library
add_subdirectory(raft)

# Broker library
add_subdirectory(broker)

# Client library
add_subdirectory(client)

# Streams library
add_subdirectory(streams)

# Connect library
add_subdirectory(connect)

# Admin library
add_subdirectory(admin)

# Main kawasan library (combines all components)
add_library(kawasan STATIC
    $<TARGET_OBJECTS:kawasan_common>
    $<TARGET_OBJECTS:kawasan_protocol>
    $<TARGET_OBJECTS:kawasan_storage>
    $<TARGET_OBJECTS:kawasan_raft>
    $<TARGET_OBJECTS:kawasan_broker>
    $<TARGET_OBJECTS:kawasan_client>
    $<TARGET_OBJECTS:kawasan_streams>
    $<TARGET_OBJECTS:kawasan_connect>
    $<TARGET_OBJECTS:kawasan_admin>
)

target_link_libraries(kawasan PUBLIC
    kawasan_monitoring
    Threads::Threads
    Boost::program_options
    RocksDB::RocksDB
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    ZLIB::ZLIB
    Snappy::snappy
    lz4::lz4
    zstd::libzstd
    OpenSSL::SSL
    OpenSSL::Crypto
)

# Install library
install(TARGETS kawasan
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)
