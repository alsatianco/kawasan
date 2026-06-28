// Phase A3: production-mode config validation. When deployment.mode=production,
// the broker must fail fast on settings it cannot honor (TLS, RF>1, minISR>1)
// instead of silently degrading them. Outside production mode the same settings
// are tolerated (with the existing warn-and-clamp behavior) so dev/test is
// unaffected.
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <string>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"

namespace {

std::string makeLogDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto tmp = std::filesystem::temp_directory_path() /
               ("kawasan-prodcfg-" + std::to_string(ts));
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

kawasan::Config baseConfig(const std::string& log_dir) {
    kawasan::Config config;
    config.setInt("broker.id", 1);
    config.setString("host", "127.0.0.1");
    config.setInt("port", 0);
    config.setInt("raft.port", 0);
    config.setInt("monitoring.port", 0);
    config.setString("log.dirs", log_dir);
    config.setInt("network.io_threads", 1);
    return config;
}

}  // namespace

TEST(ProductionConfigTest, RejectsReplicationFactorGreaterThanOne) {
    ensureLogger();
    const auto dir = makeLogDir();
    auto config = baseConfig(dir);
    config.setString("deployment.mode", "production");
    config.setInt("default.replication.factor", 3);
    EXPECT_THROW(kawasan::broker::KawasanBroker broker(config), std::runtime_error);
    std::filesystem::remove_all(dir);
}

TEST(ProductionConfigTest, RejectsMinInsyncReplicasGreaterThanOne) {
    ensureLogger();
    const auto dir = makeLogDir();
    auto config = baseConfig(dir);
    config.setString("deployment.mode", "production");
    config.setInt("min.insync.replicas", 2);
    EXPECT_THROW(kawasan::broker::KawasanBroker broker(config), std::runtime_error);
    std::filesystem::remove_all(dir);
}

TEST(ProductionConfigTest, RejectsTlsImplyingProtocol) {
    ensureLogger();
    const auto dir = makeLogDir();
    auto config = baseConfig(dir);
    config.setString("deployment.mode", "production");
    config.setString("security.protocol", "SASL_SSL");
    EXPECT_THROW(kawasan::broker::KawasanBroker broker(config), std::runtime_error);
    std::filesystem::remove_all(dir);
}

TEST(ProductionConfigTest, AcceptsSafeSingleNodeProductionConfig) {
    ensureLogger();
    const auto dir = makeLogDir();
    auto config = baseConfig(dir);
    config.setString("deployment.mode", "production");
    config.setString("security.protocol", "PLAINTEXT");
    config.setInt("default.replication.factor", 1);
    config.setInt("min.insync.replicas", 1);
    EXPECT_NO_THROW({
        kawasan::broker::KawasanBroker broker(config);
    });
    std::filesystem::remove_all(dir);
}

// Outside production mode, RF>1 is tolerated (warn-and-clamp), so dev/test
// configs that carry Kafka-style values keep working.
TEST(ProductionConfigTest, NonProductionModeToleratesUnsupportedSettings) {
    ensureLogger();
    const auto dir = makeLogDir();
    auto config = baseConfig(dir);
    config.setInt("default.replication.factor", 3);
    EXPECT_NO_THROW({
        kawasan::broker::KawasanBroker broker(config);
    });
    std::filesystem::remove_all(dir);
}
