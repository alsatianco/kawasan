add_executable(tcp_front_door_test tcp_front_door_test.cpp)
target_link_libraries(tcp_front_door_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME TcpFrontDoorTest COMMAND tcp_front_door_test)

add_executable(fetch_path_test fetch_path_test.cpp)
target_link_libraries(fetch_path_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME FetchPathTest COMMAND fetch_path_test)

add_executable(log_recovery_test log_recovery_test.cpp)
target_link_libraries(log_recovery_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME LogRecoveryTest COMMAND log_recovery_test)

add_executable(internal_topic_fd_test internal_topic_fd_test.cpp)
target_link_libraries(internal_topic_fd_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME InternalTopicFdTest COMMAND internal_topic_fd_test)

add_executable(idempotent_replay_test idempotent_replay_test.cpp)
target_link_libraries(idempotent_replay_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME IdempotentReplayTest COMMAND idempotent_replay_test)

add_executable(production_config_test production_config_test.cpp)
target_link_libraries(production_config_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ProductionConfigTest COMMAND production_config_test)

add_executable(acl_enforcement_test acl_enforcement_test.cpp)
target_link_libraries(acl_enforcement_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME AclEnforcementTest COMMAND acl_enforcement_test)

# Phase EX-10 / opus2 Item 1: V2 batch header round-trip through
# appendBatch + RocksDB persist + reopen. Locks the invariant that
# producer_id, producer_epoch, base_sequence, isTransactional, and
# isControl all survive recovery — required for read_committed.
add_executable(transactional_recovery_test transactional_recovery_test.cpp)
target_link_libraries(transactional_recovery_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME TransactionalRecoveryTest COMMAND transactional_recovery_test)

# M1: durable transaction state (__transaction_state persist + replay) across
# a simulated restart, covering each crash point in the txn lifecycle.
add_executable(transaction_state_recovery_test transaction_state_recovery_test.cpp)
target_link_libraries(transaction_state_recovery_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME TransactionStateRecoveryTest COMMAND transaction_state_recovery_test)

# M2: producer-epoch fencing + transaction-timeout auto-abort, over a real
# socket against an in-process broker.
add_executable(transaction_fencing_test transaction_fencing_test.cpp)
target_link_libraries(transaction_fencing_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME TransactionFencingTest COMMAND transaction_fencing_test)

add_executable(replica_fetch_test replica_fetch_test.cpp)
target_link_libraries(replica_fetch_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ReplicaFetchTest COMMAND replica_fetch_test)

# M8-E2: Fetch / ListOffsets current_leader_epoch fencing
add_executable(leader_epoch_fencing_test leader_epoch_fencing_test.cpp)
target_link_libraries(leader_epoch_fencing_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME LeaderEpochFencingTest COMMAND leader_epoch_fencing_test)

# Broker-to-broker client (replication, AlterPartition, OffsetForLeaderEpoch)
add_executable(peer_client_test peer_client_test.cpp)
target_link_libraries(peer_client_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME PeerClientTest COMMAND peer_client_test)

# InitProducerId allocation (single-node sequence, per-broker ranges in a cluster)
add_executable(producer_id_allocation_test producer_id_allocation_test.cpp)
target_link_libraries(producer_id_allocation_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ProducerIdAllocationTest COMMAND producer_id_allocation_test)

add_executable(admin_surface_test admin_surface_test.cpp)
target_link_libraries(admin_surface_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME AdminSurfaceTest COMMAND admin_surface_test)

add_executable(produce_acks_test produce_acks_test.cpp)
target_link_libraries(produce_acks_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ProduceAcksTest COMMAND produce_acks_test)

# Shutdown test - temporarily disabled pending protocol header cleanup
# add_executable(shutdown_test shutdown_test.cpp)
# target_link_libraries(shutdown_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
# add_test(NAME ShutdownTest COMMAND shutdown_test)

# Disabled until Consumer/Producer clients are implemented
# add_executable(offset_persistence_test offset_persistence_test.cpp)
# target_link_libraries(offset_persistence_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
# add_test(NAME OffsetPersistenceTest COMMAND offset_persistence_test)

# Raft metadata replication test (requires start_cluster.sh)
# 0A.9: bake the absolute source dir so the test can locate scripts/ from any cwd.
add_executable(raft_metadata_test raft_metadata_test.cpp)
target_compile_definitions(raft_metadata_test PRIVATE
    KAWASAN_SOURCE_DIR="${CMAKE_SOURCE_DIR}")
target_link_libraries(raft_metadata_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME RaftMetadataTest COMMAND raft_metadata_test
         WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}")

# Replication and failover test (requires start_cluster.sh)
add_executable(replication_test replication_test.cpp)
target_link_libraries(replication_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ReplicationTest COMMAND replication_test)

# Streams stateful operations test
add_executable(streams_stateful_test streams_stateful_test.cpp)
target_link_libraries(streams_stateful_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME StreamsStatefulTest COMMAND streams_stateful_test)

# Streams windowing test (Week 20)
add_executable(streams_windowing_test streams_windowing_test.cpp)
target_link_libraries(streams_windowing_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME StreamsWindowingTest COMMAND streams_windowing_test)

# Streams joins test (Week 21)
add_executable(streams_joins_test streams_joins_test.cpp)
target_link_libraries(streams_joins_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME StreamsJoinsTest COMMAND streams_joins_test)

# Streams comprehensive test (Week 22)
add_executable(streams_comprehensive_test streams_comprehensive_test.cpp)
target_link_libraries(streams_comprehensive_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME StreamsComprehensiveTest COMMAND streams_comprehensive_test)

# Connect framework test (Week 23)
add_executable(connect_test connect_test.cpp)
target_link_libraries(connect_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ConnectTest COMMAND connect_test)

# Connect source connector test (Week 24)
add_executable(connect_source_test connect_source_test.cpp)
target_link_libraries(connect_source_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ConnectSourceTest COMMAND connect_source_test)

# M8-G: coordinator discovery and ownership over real cluster sockets.
add_executable(find_coordinator_routing_test find_coordinator_routing_test.cpp)
target_link_libraries(find_coordinator_routing_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME FindCoordinatorRoutingTest COMMAND find_coordinator_routing_test)

# M9 real process death: exact acknowledgements and durable producer/txn state.
add_executable(chaos_kill_test chaos_kill_test.cpp)
target_link_libraries(chaos_kill_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ChaosKillTest COMMAND chaos_kill_test)
set_tests_properties(ChaosKillTest PROPERTIES TIMEOUT 60)

# CM-3: actual advertisement, dispatcher profile caps and UUID/offset wire semantics.
add_executable(client_api_parity_test client_api_parity_test.cpp)
target_link_libraries(client_api_parity_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME ClientApiParityTest COMMAND client_api_parity_test)
set_tests_properties(ClientApiParityTest PROPERTIES TIMEOUT 90)

# CM-4: real group metadata fencing before transactional offsets are staged.
add_executable(txn_offset_fencing_test txn_offset_fencing_test.cpp)
target_link_libraries(txn_offset_fencing_test PRIVATE kawasan GTest::gtest GTest::gtest_main)
add_test(NAME TxnOffsetFencingTest COMMAND txn_offset_fencing_test)
