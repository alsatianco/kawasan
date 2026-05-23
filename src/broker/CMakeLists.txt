# Add monitoring subdirectory
add_subdirectory(monitoring)

add_library(kawasan_broker OBJECT
    kawasan_broker.cpp
    tcp_server.cpp
    request_dispatcher.cpp
    request_metrics.cpp
    metadata_store.cpp
    metadata_controller.cpp
    group_coordinator.cpp
    offset_manager.cpp
    replica_manager.cpp
    producer_state_manager.cpp
    scram_auth.cpp
    acl_store.cpp
    transaction_coordinator.cpp
    fetch_session_manager.cpp
    isolation_tracker.cpp
)

target_include_directories(kawasan_broker PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kawasan_broker PUBLIC
    kawasan_common
    kawasan_protocol
    kawasan_storage
    kawasan_raft
    kawasan_monitoring
    Threads::Threads
)
