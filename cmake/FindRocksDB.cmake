# FindRocksDB.cmake
# Finds the RocksDB library
#
# This will define the following variables:
#   RocksDB_FOUND - System has RocksDB
#   RocksDB_INCLUDE_DIRS - The RocksDB include directories
#   RocksDB_LIBRARIES - The libraries needed to use RocksDB
#
# and the following imported targets:
#   RocksDB::RocksDB - The RocksDB library

find_path(RocksDB_INCLUDE_DIR
    NAMES rocksdb/db.h
    PATHS
        /usr/include
        /usr/local/include
        ${ROCKSDB_ROOT}/include
)

find_library(RocksDB_LIBRARY
    NAMES rocksdb
    PATHS
        /usr/lib
        /usr/local/lib
        ${ROCKSDB_ROOT}/lib
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RocksDB
    REQUIRED_VARS RocksDB_LIBRARY RocksDB_INCLUDE_DIR
)

if(RocksDB_FOUND)
    set(RocksDB_LIBRARIES ${RocksDB_LIBRARY})
    set(RocksDB_INCLUDE_DIRS ${RocksDB_INCLUDE_DIR})

    if(NOT TARGET RocksDB::RocksDB)
        add_library(RocksDB::RocksDB UNKNOWN IMPORTED)
        set_target_properties(RocksDB::RocksDB PROPERTIES
            IMPORTED_LOCATION "${RocksDB_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${RocksDB_INCLUDE_DIR}"
        )
    endif()
endif()

mark_as_advanced(RocksDB_INCLUDE_DIR RocksDB_LIBRARY)

