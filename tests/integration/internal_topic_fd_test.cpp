// Phase A2: internal topics (__consumer_offsets, __transaction_state) used to be
// hardcoded to 50 partitions each and created eagerly at startup. With one
// RocksDB instance per partition that is ~100 open databases (and many file
// descriptors) before a single user topic exists — a real FD-exhaustion hazard
// under tight ulimits. These tests lock in that (a) the default startup
// footprint is bounded well below the old 100, and (b) the counts are
// configurable so a constrained deployment can shrink them further.
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"

namespace {

std::string makeLogDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto tmp = std::filesystem::temp_directory_path() /
               ("kawasan-fd-test-" + std::to_string(ts));
    std::filesystem::create_directories(tmp);
    return tmp.string();
}

void ensureLogger() {
    static bool initialized = false;
    if (!initialized) {
        kawasan::Logger::init("warn");
        initialized = true;
    }
}

kawasan::Config makeConfig(const std::string& log_dir) {
    kawasan::Config config;
    config.setInt("broker.id", 1);
    config.setString("host", "127.0.0.1");
    config.setInt("port", 0);
    config.setInt("raft.port", 0);
    config.setString("log.dirs", log_dir);
    config.setInt("network.io_threads", 1);
    config.setInt("monitoring.port", 0);  // ephemeral, avoid 9094 collisions in parallel ctest
    return config;
}

}  // namespace

// The old hardcoded 50+50 layout opened ~100 RocksDB instances at startup.
// The new default (16+16) keeps the startup storage footprint well bounded.
TEST(InternalTopicFdTest, DefaultStartupFootprintIsBounded) {
    ensureLogger();
    const auto log_dir = makeLogDir();
    auto config = makeConfig(log_dir);

    kawasan::broker::KawasanBroker broker(config);
    broker.start();
    ASSERT_GT(broker.port(), 0);

    const size_t open_logs = broker.openLogCount();
    EXPECT_GT(open_logs, 0u);
    // Old behavior would be ~100 (50 offsets + 50 txn). Assert we are far below.
    EXPECT_LT(open_logs, 64u) << "internal-topic startup footprint regressed toward the old 50+50";

    broker.stop();
    std::filesystem::remove_all(log_dir);
}

// A constrained single-node deployment can shrink internal topics to cut the FD
// footprint further; the broker must honor the config keys.
TEST(InternalTopicFdTest, InternalTopicPartitionsAreConfigurable) {
    ensureLogger();
    const auto log_dir = makeLogDir();
    auto config = makeConfig(log_dir);
    config.setInt("offsets.topic.num.partitions", 2);
    config.setInt("transaction.state.topic.num.partitions", 2);

    kawasan::broker::KawasanBroker broker(config);
    broker.start();
    ASSERT_GT(broker.port(), 0);

    // 2 offsets + 2 txn = 4 internal partition logs; allow a little slack for
    // any other internal log that may be opened during startup.
    EXPECT_LE(broker.openLogCount(), 8u);

    broker.stop();
    std::filesystem::remove_all(log_dir);
}
