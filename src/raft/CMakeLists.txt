add_library(kawasan_raft OBJECT
    raft_node.cpp
    raft_protocol.cpp
    raft_transport.cpp
)

target_include_directories(kawasan_raft PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kawasan_raft PUBLIC
    kawasan_common
    kawasan_storage
    Threads::Threads
)

