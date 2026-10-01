add_library(kawasan_storage OBJECT
    record_batch.cpp
    lz4_decoder.cpp
    log_segment.cpp
    log.cpp
    log_manager.cpp
    leader_epoch_cache.cpp
)

target_include_directories(kawasan_storage PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kawasan_storage PUBLIC
    kawasan_common
    RocksDB::RocksDB
    z  # zlib for CRC32/GZIP
    Snappy::snappy
    lz4::lz4
    zstd::libzstd
)
