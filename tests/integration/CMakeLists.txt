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
