# Buffer test
add_executable(buffer_test buffer_test.cpp)
target_link_libraries(buffer_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME BufferTest COMMAND buffer_test)

# Config test (accessor coercion)
add_executable(config_test config_test.cpp)
target_link_libraries(config_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ConfigTest COMMAND config_test)

# Record batch test
add_executable(record_batch_test record_batch_test.cpp)
target_link_libraries(record_batch_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME RecordBatchTest COMMAND record_batch_test)

# Kafka broker error test
add_executable(kawasan_broker_error_test kawasan_broker_error_test.cpp)
target_link_libraries(kawasan_broker_error_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME KawasanBrokerErrorTest COMMAND kawasan_broker_error_test)

# Log segment test
add_executable(log_segment_test log_segment_test.cpp)
target_link_libraries(log_segment_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME LogSegmentTest COMMAND log_segment_test)

# Log durability test (Phase A1: sync default + atomic checkpoint)
add_executable(log_durability_test log_durability_test.cpp)
target_link_libraries(log_durability_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME LogDurabilityTest COMMAND log_durability_test)

# Log compaction active-segment guard + concurrency stress (P2)
add_executable(log_compaction_race_test log_compaction_race_test.cpp)
target_link_libraries(log_compaction_race_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME LogCompactionRaceTest COMMAND log_compaction_race_test)

# ListOffsets MAX_TIMESTAMP backend: control batches excluded (P2 rider)
add_executable(log_max_timestamp_test log_max_timestamp_test.cpp)
target_link_libraries(log_max_timestamp_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME LogMaxTimestampTest COMMAND log_max_timestamp_test)

# Golden-bytes lock for the v0-flexible admin APIs (CM-2)
add_executable(admin_stubs_wire_test admin_stubs_wire_test.cpp)
target_link_libraries(admin_stubs_wire_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME AdminStubsWireTest COMMAND admin_stubs_wire_test)

# Socket tuning on accepted connections (P3)
add_executable(tcp_socket_tuning_test tcp_socket_tuning_test.cpp)
target_link_libraries(tcp_socket_tuning_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME TcpSocketTuningTest COMMAND tcp_socket_tuning_test)

# Decoder array-length bounds (P13 — OOM-DoS guard found by fuzzing)
add_executable(decode_bounds_test decode_bounds_test.cpp)
target_link_libraries(decode_bounds_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME DecodeBoundsTest COMMAND decode_bounds_test)

# Compressed-batch re-serialization round-trip (compressed-fetch interop fix)
add_executable(record_batch_compression_test record_batch_compression_test.cpp)
target_link_libraries(record_batch_compression_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME RecordBatchCompressionTest COMMAND record_batch_compression_test)

# Transaction state manager serialize/route + coordinator two-phase (M1)
add_executable(transaction_state_manager_test transaction_state_manager_test.cpp)
target_link_libraries(transaction_state_manager_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME TransactionStateManagerTest COMMAND transaction_state_manager_test)

# Quota manager test (Phase A6: client byte-rate throttling)
add_executable(quota_manager_test quota_manager_test.cpp)
target_link_libraries(quota_manager_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME QuotaManagerTest COMMAND quota_manager_test)

# Raft protocol test
add_executable(raft_protocol_test raft_protocol_test.cpp)
target_link_libraries(raft_protocol_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME RaftProtocolTest COMMAND raft_protocol_test)

# Raft election regression test (Phase B: stale-term vote-counting fix)
add_executable(raft_election_test raft_election_test.cpp)
target_link_libraries(raft_election_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME RaftElectionTest COMMAND raft_election_test)

# Replica assignment test (Phase B: cross-broker RF>1 assignment)
add_executable(replica_assignment_test replica_assignment_test.cpp)
target_link_libraries(replica_assignment_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ReplicaAssignmentTest COMMAND replica_assignment_test)

# Raft apply-thread test (Phase B: commit-apply off the log mutex)
add_executable(raft_apply_thread_test raft_apply_thread_test.cpp)
target_link_libraries(raft_apply_thread_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME RaftApplyThreadTest COMMAND raft_apply_thread_test)

# Raft transport test
add_executable(raft_transport_test raft_transport_test.cpp)
target_link_libraries(raft_transport_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME RaftTransportTest COMMAND raft_transport_test)

# ISR management test
add_executable(isr_management_test isr_management_test.cpp)
target_link_libraries(isr_management_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ISRManagementTest COMMAND isr_management_test)

# Streams topology test
add_executable(streams_topology_test streams_topology_test.cpp)
target_link_libraries(streams_topology_test PRIVATE kawasan_streams kawasan GTest::gtest GTest::gtest_main)
add_test(NAME StreamsTopologyTest COMMAND streams_topology_test)

# Phase 2.1: ProducerStateManager (idempotent producer dedup)
add_executable(producer_state_manager_test producer_state_manager_test.cpp)
target_link_libraries(producer_state_manager_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ProducerStateManagerTest COMMAND producer_state_manager_test)

# Phase 4.2c: AclStore
add_executable(acl_store_test acl_store_test.cpp)
target_link_libraries(acl_store_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME AclStoreTest COMMAND acl_store_test)

# Phase 4.2b: SCRAM-SHA-256/512 authentication
add_executable(scram_auth_test scram_auth_test.cpp)
target_link_libraries(scram_auth_test PRIVATE kawasan OpenSSL::Crypto GTest::gtest GTest::gtest_main)
add_test(NAME ScramAuthTest COMMAND scram_auth_test)

# Phase 1.4: FetchSessionManager
add_executable(fetch_session_manager_test fetch_session_manager_test.cpp)
target_link_libraries(fetch_session_manager_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME FetchSessionManagerTest COMMAND fetch_session_manager_test)

# Phase 5.2: InstallSnapshot wire-format
add_executable(install_snapshot_test install_snapshot_test.cpp)
target_link_libraries(install_snapshot_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME InstallSnapshotTest COMMAND install_snapshot_test)

# Phase 3.3: TransactionCoordinator state machine
add_executable(transaction_coordinator_test transaction_coordinator_test.cpp)
target_link_libraries(transaction_coordinator_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME TransactionCoordinatorTest COMMAND transaction_coordinator_test)

# Phase EX-1 (§6.3): required metrics contract
add_executable(required_metrics_test required_metrics_test.cpp)
target_link_libraries(required_metrics_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME RequiredMetricsTest COMMAND required_metrics_test)

# Phase EX-10: read_committed isolation tracker
add_executable(isolation_tracker_test isolation_tracker_test.cpp)
target_link_libraries(isolation_tracker_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME IsolationTrackerTest COMMAND isolation_tracker_test)

# Phase EX-3: schema-driven codegen byte-for-byte equivalence
add_executable(codegen_golden_bytes_test codegen_golden_bytes_test.cpp)
target_link_libraries(codegen_golden_bytes_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME CodegenGoldenBytesTest COMMAND codegen_golden_bytes_test)

# Phase EX-12: GroupCoordinator protocol echo + rebalance-timeout recovery
add_executable(group_coordinator_test group_coordinator_test.cpp)
target_link_libraries(group_coordinator_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME GroupCoordinatorTest COMMAND group_coordinator_test)
