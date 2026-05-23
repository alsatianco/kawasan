/**
 * Streams Anomaly Detection Example
 *
 * Demonstrates real-time anomaly detection using:
 * - Windowed aggregations to compute statistics
 * - Stateful processing to maintain baseline metrics
 * - Punctuations for periodic model updates
 * - Custom processors for anomaly scoring
 *
 * Use case: Detect unusual transaction patterns in payment data
 *
 * Equivalent Kafka Streams concept:
 *   KStream<String, Transaction> transactions = builder.stream("transactions");
 *
 *   // Compute per-user statistics in sliding windows
 *   transactions
 *       .groupByKey()
 *       .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(...))
 *       .aggregate(...)
 *
 *   // Detect anomalies by comparing to baseline
 *   transactions.transformValues(() -> new AnomalyDetector())
 *               .filter((k, v) -> v.isAnomaly())
 *               .to("anomalies");
 *
 * Usage:
 *   ./examples/streams_anomaly
 */

#include "kawasan/streams/streams_builder.h"
#include "kawasan/streams/topology.h"
#include "kawasan/streams/processor.h"
#include "kawasan/streams/processor_context.h"
#include "kawasan/streams/windows.h"
#include "kawasan/streams/json_serde.h"
#include <iostream>
#include <map>
#include <vector>
#include <set>
#include <cmath>
#include <numeric>
#include <algorithm>
#include <random>
#include <iomanip>

using namespace kawasan::streams;

// Domain objects
struct Transaction {
    std::string transaction_id;
    std::string user_id;
    std::string merchant;
    double amount;
    int64_t timestamp;
    std::string location;
};

struct UserStats {
    double mean_amount;
    double std_dev;
    int64_t transaction_count;
    double total_amount;
    int64_t last_update;
};

struct AnomalyResult {
    Transaction transaction;
    double anomaly_score;
    std::string reason;
    bool is_anomaly;
};

// JSON serialization
void to_json(nlohmann::json& j, const Transaction& t) {
    j = nlohmann::json{
        {"transaction_id", t.transaction_id},
        {"user_id", t.user_id},
        {"merchant", t.merchant},
        {"amount", t.amount},
        {"timestamp", t.timestamp},
        {"location", t.location}
    };
}

void from_json(const nlohmann::json& j, Transaction& t) {
    j.at("transaction_id").get_to(t.transaction_id);
    j.at("user_id").get_to(t.user_id);
    j.at("merchant").get_to(t.merchant);
    j.at("amount").get_to(t.amount);
    j.at("timestamp").get_to(t.timestamp);
    j.at("location").get_to(t.location);
}

/**
 * Anomaly detection processor using statistical analysis
 *
 * Maintains per-user statistics and detects anomalies based on:
 * 1. Amount deviation from mean (z-score > 3)
 * 2. Unusual transaction velocity (too many in short time)
 * 3. Geographic anomalies (unusual location patterns)
 */
class AnomalyDetectorProcessor : public Processor<std::string, Transaction> {
public:
    static constexpr double ZSCORE_THRESHOLD = 3.0;
    static constexpr int MIN_TRANSACTIONS_FOR_BASELINE = 5;
    static constexpr int64_t VELOCITY_WINDOW_MS = 60000;  // 1 minute
    static constexpr int MAX_TRANSACTIONS_PER_MINUTE = 10;

    void init(ProcessorContext& ctx) override {
        Processor::init(ctx);

        // Schedule periodic stats decay (every hour)
        ctx.schedule(
            Duration(3600000),
            PunctuationType::WALL_CLOCK_TIME,
            [this](int64_t timestamp) {
                decayOldStats(timestamp);
            }
        );

        std::cout << "AnomalyDetector initialized with thresholds:\n"
                  << "  Z-score threshold: " << ZSCORE_THRESHOLD << "\n"
                  << "  Velocity limit: " << MAX_TRANSACTIONS_PER_MINUTE << " tx/min\n";
    }

    void process(const std::string& userId, const Transaction& tx) override {
        auto& stats = userStats_[userId];
        auto& history = userHistory_[userId];

        std::vector<std::string> anomalyReasons;
        double anomalyScore = 0.0;

        // Check 1: Amount anomaly (z-score)
        if (stats.transaction_count >= MIN_TRANSACTIONS_FOR_BASELINE && stats.std_dev > 0) {
            double zscore = (tx.amount - stats.mean_amount) / stats.std_dev;
            if (std::abs(zscore) > ZSCORE_THRESHOLD) {
                anomalyScore += std::abs(zscore) / ZSCORE_THRESHOLD;
                anomalyReasons.push_back("amount_deviation(z=" +
                    std::to_string(zscore).substr(0, 5) + ")");
            }
        }

        // Check 2: Velocity anomaly
        int recentCount = 0;
        for (const auto& hist : history) {
            if (tx.timestamp - hist.timestamp < VELOCITY_WINDOW_MS) {
                recentCount++;
            }
        }
        if (recentCount >= MAX_TRANSACTIONS_PER_MINUTE) {
            anomalyScore += static_cast<double>(recentCount) / MAX_TRANSACTIONS_PER_MINUTE;
            anomalyReasons.push_back("high_velocity(" +
                std::to_string(recentCount + 1) + "/min)");
        }

        // Check 3: Location anomaly (simplified)
        if (!history.empty()) {
            std::set<std::string> recentLocations;
            for (const auto& hist : history) {
                if (tx.timestamp - hist.timestamp < 3600000) {  // Last hour
                    recentLocations.insert(hist.location);
                }
            }
            if (!recentLocations.empty() && recentLocations.find(tx.location) == recentLocations.end()) {
                if (recentLocations.size() >= 2) {
                    anomalyScore += 0.5;
                    anomalyReasons.push_back("unusual_location");
                }
            }
        }

        // Check 4: Large amount threshold
        if (tx.amount > 10000) {
            anomalyScore += tx.amount / 10000.0;
            anomalyReasons.push_back("large_amount($" +
                std::to_string(static_cast<int>(tx.amount)) + ")");
        }

        // Update statistics
        updateStats(userId, tx);

        // Store result
        bool isAnomaly = anomalyScore >= 1.0;
        std::string reason = anomalyReasons.empty() ? "none" :
            std::accumulate(anomalyReasons.begin(), anomalyReasons.end(), std::string(),
                [](const std::string& a, const std::string& b) {
                    return a.empty() ? b : a + ", " + b;
                });

        results_.push_back({tx, anomalyScore, reason, isAnomaly});

        if (isAnomaly) {
            std::cout << "  ANOMALY DETECTED: " << tx.transaction_id
                      << " (score=" << std::fixed << std::setprecision(2) << anomalyScore
                      << ", reason: " << reason << ")\n";
        }
    }

    void close() override {
        std::cout << "AnomalyDetector closing. Processed " << getTotalProcessed()
                  << " transactions, detected " << getAnomalyCount() << " anomalies.\n";
    }

    const std::vector<AnomalyResult>& getResults() const { return results_; }
    int getTotalProcessed() const { return results_.size(); }
    int getAnomalyCount() const {
        return std::count_if(results_.begin(), results_.end(),
                            [](const AnomalyResult& r) { return r.is_anomaly; });
    }

private:
    void updateStats(const std::string& userId, const Transaction& tx) {
        auto& stats = userStats_[userId];
        auto& history = userHistory_[userId];

        // Update running statistics (Welford's algorithm for online variance)
        stats.transaction_count++;
        stats.total_amount += tx.amount;

        double delta = tx.amount - stats.mean_amount;
        stats.mean_amount += delta / stats.transaction_count;
        double delta2 = tx.amount - stats.mean_amount;

        if (stats.transaction_count > 1) {
            double variance = ((stats.transaction_count - 1) * std::pow(stats.std_dev, 2) +
                              delta * delta2) / stats.transaction_count;
            stats.std_dev = std::sqrt(variance);
        }

        stats.last_update = tx.timestamp;

        // Keep recent history (last 100 transactions)
        history.push_back(tx);
        if (history.size() > 100) {
            history.erase(history.begin());
        }
    }

    void decayOldStats(int64_t currentTime) {
        // Decay old stats to reduce impact of very old data
        for (auto& [userId, stats] : userStats_) {
            if (currentTime - stats.last_update > 86400000) {  // > 1 day old
                stats.std_dev *= 0.9;  // Decay variance
            }
        }
    }

    std::map<std::string, UserStats> userStats_;
    std::map<std::string, std::vector<Transaction>> userHistory_;
    std::vector<AnomalyResult> results_;
};

int main(int argc, char* argv[]) {
    std::cout << "=== Kawasan Streams Anomaly Detection Example ===\n\n";

    // Parse command line arguments
    int num_transactions = 50;
    double anomaly_rate = 0.15;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--transactions" && i + 1 < argc) {
            num_transactions = std::stoi(argv[++i]);
        } else if (arg == "--anomaly-rate" && i + 1 < argc) {
            anomaly_rate = std::stod(argv[++i]);
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  --transactions N    Number of transactions (default: 50)\n"
                      << "  --anomaly-rate R    Rate of anomalous transactions (default: 0.15)\n"
                      << "  --help              Show this help\n";
            return 0;
        }
    }

    std::cout << "Configuration:\n"
              << "  Transactions: " << num_transactions << "\n"
              << "  Anomaly rate: " << (anomaly_rate * 100) << "%\n\n";

    // Build the topology
    StreamsBuilder builder;

    // Read transactions stream
    auto transactions = builder.stream<std::string, std::string>("transactions");

    // Apply anomaly detection transformation
    // In a real implementation, this would use transformValues with custom processor
    auto analyzed = transactions.mapValues<std::string>(
        [](const std::string& txJson) {
            return txJson;  // Placeholder for actual transformation
        }
    );

    // Filter anomalies
    auto anomalies = analyzed.filter(
        [](const std::string&, const std::string&) {
            return true;  // Placeholder - actual filter in processor
        }
    );

    // Build the topology
    Topology topology = builder.build();

    std::cout << "Topology:\n";
    std::cout << "=========\n";
    std::cout << topology.describe() << "\n";

    // Generate sample data and process
    std::cout << "Processing Sample Transactions:\n";
    std::cout << "===============================\n";

    // Sample users and merchants
    std::vector<std::string> users = {"user_alice", "user_bob", "user_carol", "user_david"};
    std::vector<std::string> merchants = {"Amazon", "Walmart", "Starbucks", "Gas Station", "Restaurant"};
    std::vector<std::string> locations = {"New York", "Los Angeles", "Chicago", "Houston"};

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> amount_normal(20.0, 200.0);
    std::uniform_real_distribution<> amount_anomaly(5000.0, 50000.0);
    std::uniform_int_distribution<> user_dist(0, users.size() - 1);
    std::uniform_int_distribution<> merchant_dist(0, merchants.size() - 1);
    std::uniform_int_distribution<> location_dist(0, locations.size() - 1);
    std::uniform_real_distribution<> anomaly_chance(0.0, 1.0);

    // Create anomaly detector
    AnomalyDetectorProcessor detector;
    ProcessorContext context;
    context.setIds("anomaly-detection-app", "task-0");
    detector.init(context);

    int64_t timestamp = 1700000000000;
    std::map<std::string, std::string> userLocations;  // Track usual locations

    for (int i = 0; i < num_transactions; ++i) {
        bool isAnomalousTransaction = anomaly_chance(gen) < anomaly_rate;

        std::string userId = users[user_dist(gen)];
        std::string merchant = merchants[merchant_dist(gen)];

        // Determine location (anomaly = different location)
        std::string location;
        if (userLocations.find(userId) == userLocations.end()) {
            location = locations[location_dist(gen)];
            userLocations[userId] = location;
        } else if (isAnomalousTransaction && anomaly_chance(gen) < 0.3) {
            // Location anomaly
            do {
                location = locations[location_dist(gen)];
            } while (location == userLocations[userId]);
        } else {
            location = userLocations[userId];
        }

        // Determine amount (anomaly = large or unusual amount)
        double amount;
        if (isAnomalousTransaction) {
            amount = amount_anomaly(gen);
        } else {
            amount = amount_normal(gen);
        }

        // Create transaction
        Transaction tx{
            "tx_" + std::to_string(i),
            userId,
            merchant,
            amount,
            timestamp,
            location
        };

        std::cout << "  " << tx.transaction_id << ": " << tx.user_id
                  << " -> " << tx.merchant << " $" << std::fixed << std::setprecision(2)
                  << tx.amount << " @ " << tx.location;

        if (isAnomalousTransaction) {
            std::cout << " [INJECTED ANOMALY]";
        }
        std::cout << "\n";

        // Process transaction
        detector.process(userId, tx);

        // Advance time
        timestamp += std::uniform_int_distribution<>(1000, 60000)(gen);
    }

    detector.close();

    // Print summary
    std::cout << "\n=== Summary ===\n";
    std::cout << "Total transactions: " << detector.getTotalProcessed() << "\n";
    std::cout << "Anomalies detected: " << detector.getAnomalyCount() << "\n";
    std::cout << "Detection rate: " << std::fixed << std::setprecision(1)
              << (100.0 * detector.getAnomalyCount() / detector.getTotalProcessed()) << "%\n";

    // Show anomaly details
    std::cout << "\nDetected Anomalies:\n";
    std::cout << "-------------------\n";
    for (const auto& result : detector.getResults()) {
        if (result.is_anomaly) {
            std::cout << "  " << result.transaction.transaction_id
                      << ": score=" << std::fixed << std::setprecision(2) << result.anomaly_score
                      << ", reason: " << result.reason << "\n";
        }
    }

    std::cout << "\n=== Anomaly Detection Example Complete ===\n";

    return 0;
}
