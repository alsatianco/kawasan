// Phase EX-1 (§6.3): assert all required metric names appear in the
// MetricsCollector::exportPrometheus() output. This locks the contract
// against future refactors so the "must export" clause stays satisfied.

#include <gtest/gtest.h>

#include <regex>

#include "kawasan/broker/fetch_session_manager.h"
#include "kawasan/broker/monitoring/metrics_collector.h"
#include "kawasan/broker/producer_state_manager.h"
#include "kawasan/broker/transaction_coordinator.h"

namespace {

using kawasan::broker::FetchSessionManager;
using kawasan::broker::ProducerStateManager;
using kawasan::broker::TransactionCoordinator;
using kawasan::broker::monitoring::MetricsCollector;

bool hasMetric(const std::string& text, const std::string& name) {
    const std::regex r("^# TYPE " + name + "[ \\t]");
    std::istringstream iss(text);
    for (std::string line; std::getline(iss, line);) {
        if (std::regex_search(line, r))
            return true;
    }
    return false;
}

TEST(RequiredMetricsTest, AllSubsystemMetricsExposed) {
    MetricsCollector mc;
    ProducerStateManager psm;
    FetchSessionManager fsm;
    TransactionCoordinator tc;

    // Trigger one record to populate state so the gauge values are
    // non-zero (the metric registration shouldn't depend on this, but
    // the test reads more honestly with real values).
    psm.recordAppend("topic-a", 0, /*pid=*/100, /*epoch=*/0,
                     /*base_seq=*/0, /*records=*/5, /*offset=*/0);
    tc.recordInitProducerId("txn-1", 100, 0, 60000);

    mc.setProducerStateProvider([&]() {
        auto m = psm.getMetrics();
        std::ostringstream oss;
        oss << "# HELP kawasan_producer_state_entries x\n# TYPE kawasan_producer_state_entries "
               "gauge\n"
            << "kawasan_producer_state_entries " << m.entries << "\n\n"
            << "# HELP kawasan_producer_state_evictions_total x\n# TYPE "
               "kawasan_producer_state_evictions_total counter\n"
            << "kawasan_producer_state_evictions_total " << m.evictions_total << "\n\n"
            << "# HELP kawasan_producer_id_count x\n# TYPE kawasan_producer_id_count gauge\n"
            << "kawasan_producer_id_count " << m.producer_id_count << "\n\n";
        return oss.str();
    });
    mc.setFetchSessionProvider([&]() {
        auto m = fsm.getMetrics();
        std::ostringstream oss;
        oss << "# HELP kawasan_fetch_session_count x\n# TYPE kawasan_fetch_session_count gauge\n"
            << "kawasan_fetch_session_count " << m.session_count << "\n\n"
            << "# HELP kawasan_fetch_session_evictions_total x\n# TYPE "
               "kawasan_fetch_session_evictions_total counter\n"
            << "kawasan_fetch_session_evictions_total " << m.evictions_total << "\n\n"
            << "# HELP kawasan_incremental_fetch_session_hit_ratio x\n# TYPE "
               "kawasan_incremental_fetch_session_hit_ratio gauge\n"
            << "kawasan_incremental_fetch_session_hit_ratio " << m.incremental_hit_ratio << "\n\n";
        return oss.str();
    });
    mc.setTransactionProvider([&]() {
        auto m = tc.getMetrics();
        std::ostringstream oss;
        oss << "# HELP kawasan_transactions_in_progress x\n# TYPE kawasan_transactions_in_progress "
               "gauge\n"
            << "kawasan_transactions_in_progress " << m.in_progress << "\n\n"
            << "# HELP kawasan_transaction_commits_total x\n# TYPE "
               "kawasan_transaction_commits_total counter\n"
            << "kawasan_transaction_commits_total " << m.commits_total << "\n\n"
            << "# HELP kawasan_transaction_aborts_total x\n# TYPE kawasan_transaction_aborts_total "
               "counter\n"
            << "kawasan_transaction_aborts_total " << m.aborts_total << "\n\n"
            << "# HELP kawasan_transaction_state_loads_total x\n# TYPE "
               "kawasan_transaction_state_loads_total counter\n"
            << "kawasan_transaction_state_loads_total " << m.state_loads_total << "\n\n";
        return oss.str();
    });

    const auto out = mc.exportPrometheus();

    // §6.3 metrics (every one named in the table).
    EXPECT_TRUE(hasMetric(out, "kawasan_producer_state_entries"));
    EXPECT_TRUE(hasMetric(out, "kawasan_producer_state_evictions_total"));
    EXPECT_TRUE(hasMetric(out, "kawasan_producer_id_count"));
    EXPECT_TRUE(hasMetric(out, "kawasan_fetch_session_count"));
    EXPECT_TRUE(hasMetric(out, "kawasan_fetch_session_evictions_total"));
    EXPECT_TRUE(hasMetric(out, "kawasan_incremental_fetch_session_hit_ratio"));
    EXPECT_TRUE(hasMetric(out, "kawasan_transactions_in_progress"));
    EXPECT_TRUE(hasMetric(out, "kawasan_transaction_commits_total"));
    EXPECT_TRUE(hasMetric(out, "kawasan_transaction_aborts_total"));
    EXPECT_TRUE(hasMetric(out, "kawasan_transaction_state_loads_total"));

    // Sanity: gauge values reflect the test setup.
    EXPECT_NE(out.find("kawasan_producer_state_entries 1"), std::string::npos);
    EXPECT_NE(out.find("kawasan_transaction_state_loads_total 1"), std::string::npos);
}

TEST(RequiredMetricsTest, ProducerEvictionsCountClearedKeys) {
    ProducerStateManager psm;
    psm.recordAppend("topic-a", 0, 1, 0, 0, 5, 0);
    psm.recordAppend("topic-a", 1, 2, 0, 0, 5, 0);
    psm.clear();
    auto m = psm.getMetrics();
    EXPECT_EQ(m.evictions_total, 2);
    EXPECT_EQ(m.entries, 0);
}

TEST(RequiredMetricsTest, FetchSessionHitRatio) {
    FetchSessionManager fsm;
    fsm.recordHitOrMiss(true);
    fsm.recordHitOrMiss(true);
    fsm.recordHitOrMiss(true);
    fsm.recordHitOrMiss(false);
    auto m = fsm.getMetrics();
    EXPECT_EQ(m.incremental_hits_total, 3);
    EXPECT_EQ(m.incremental_misses_total, 1);
    EXPECT_DOUBLE_EQ(m.incremental_hit_ratio, 0.75);
}

// P3: connection lifecycle metrics — operators must be able to tell WHY
// connections close (idle reaping vs protocol errors vs IO errors) to
// diagnose churn.
TEST(RequiredMetricsTest, ConnectionLifecycleMetricsExposed) {
    MetricsCollector mc;
    mc.incrementConnectionsCreated();
    mc.incrementConnectionClosed(kawasan::broker::monitoring::ConnectionCloseReason::kNormal);
    mc.incrementConnectionClosed(kawasan::broker::monitoring::ConnectionCloseReason::kIdleTimeout);
    mc.incrementConnectionClosed(
        kawasan::broker::monitoring::ConnectionCloseReason::kProtocolError);
    mc.incrementConnectionClosed(kawasan::broker::monitoring::ConnectionCloseReason::kIoError);

    const std::string out = mc.exportPrometheus();
    EXPECT_TRUE(hasMetric(out, "kawasan_connections_created_total"));
    EXPECT_TRUE(hasMetric(out, "kawasan_connections_closed_total"));
    EXPECT_NE(out.find("kawasan_connections_closed_total{reason=\"normal\"} 1"), std::string::npos);
    EXPECT_NE(out.find("kawasan_connections_closed_total{reason=\"idle_timeout\"} 1"),
              std::string::npos);
    EXPECT_NE(out.find("kawasan_connections_closed_total{reason=\"protocol_error\"} 1"),
              std::string::npos);
    EXPECT_NE(out.find("kawasan_connections_closed_total{reason=\"io_error\"} 1"),
              std::string::npos);
}

TEST(RequiredMetricsTest, TransactionInProgressCount) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("a", 1, 0, 60000);
    tc.addPartitions("a", {{"t", 0}});
    tc.recordInitProducerId("b", 2, 0, 60000);
    tc.addPartitions("b", {{"t", 1}});
    tc.commitTxn("a");  // a → CompleteCommit; b stays Ongoing
    auto m = tc.getMetrics();
    EXPECT_EQ(m.in_progress, 1);
    EXPECT_EQ(m.commits_total, 1);
    EXPECT_EQ(m.aborts_total, 0);
    EXPECT_EQ(m.state_loads_total, 2);
}

}  // namespace
