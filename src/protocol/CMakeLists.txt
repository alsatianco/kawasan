add_library(kawasan_protocol OBJECT
    api_keys.cpp
    request_header.cpp
    metadata_request.cpp
    produce_request.cpp
    fetch_request.cpp
    list_offsets_request.cpp
    find_coordinator_request.cpp
    api_versions.cpp
    create_topics_request.cpp
    delete_topics_request.cpp
    join_group_request.cpp
    sync_group_request.cpp
    heartbeat_request.cpp
    offset_commit_request.cpp
    offset_fetch_request.cpp
    leave_group_request.cpp
    describe_groups_request.cpp
    list_groups_request.cpp
    describe_configs_request.cpp
    describe_cluster_request.cpp
    init_producer_id_request.cpp
    offset_for_leader_epoch_request.cpp
    alter_configs_request.cpp
    incremental_alter_configs_request.cpp
    admin_misc_requests.cpp
    admin_stubs.cpp
    sasl_request.cpp
    txn_request.cpp
    # Phase EX-3: schema-driven generated encoders. Each entry here
    # corresponds to a JSON schema in tools/codegen/schemas/. Run
    # `python3 tools/codegen/generate.py --schema <foo>.json --out-dir
    # include/kawasan/protocol/generated` to regenerate; then move
    # the .cpp to src/protocol/generated/. Equivalence to hand-written
    # code is locked by tests/unit/codegen_golden_bytes_test.cpp.
    generated/ApiVersionsRequest.cpp
    generated/ApiVersionsResponse.cpp
    generated/SaslHandshakeRequest.cpp
)

target_include_directories(kawasan_protocol PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kawasan_protocol PUBLIC
    kawasan_common
)
