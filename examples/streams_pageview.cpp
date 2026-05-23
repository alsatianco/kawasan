/**
 * Streams Page View Enrichment Example
 *
 * Demonstrates stream-table join for enriching events:
 * - Page view events (stream) joined with user profiles (table)
 * - Enriches click events with user demographic data
 * - Groups by region and counts page views
 *
 * Equivalent Kafka Streams:
 *   KStream<String, PageView> pageViews = builder.stream("page-views");
 *   KTable<String, UserProfile> users = builder.table("users");
 *
 *   KStream<String, EnrichedPageView> enriched = pageViews
 *       .join(users, (pageView, user) -> new EnrichedPageView(pageView, user));
 *
 *   enriched.groupBy((key, value) -> value.getRegion())
 *           .count()
 *           .toStream()
 *           .to("pageviews-by-region");
 *
 * Usage:
 *   ./examples/streams_pageview
 */

#include "kawasan/streams/streams_builder.h"
#include "kawasan/streams/topology.h"
#include "kawasan/streams/kawasan_streams.h"
#include "kawasan/streams/json_serde.h"
#include <iostream>
#include <map>
#include <vector>
#include <chrono>
#include <iomanip>

using namespace kawasan::streams;

// Domain objects
struct PageView {
    std::string user_id;
    std::string page_url;
    int64_t timestamp;
    std::string referrer;
};

struct UserProfile {
    std::string user_id;
    std::string name;
    std::string region;
    std::string interests;
    int age;
};

struct EnrichedPageView {
    std::string user_id;
    std::string page_url;
    int64_t timestamp;
    std::string referrer;
    std::string user_name;
    std::string region;
    int age;
};

// JSON serialization for PageView
void to_json(nlohmann::json& j, const PageView& p) {
    j = nlohmann::json{
        {"user_id", p.user_id},
        {"page_url", p.page_url},
        {"timestamp", p.timestamp},
        {"referrer", p.referrer}
    };
}

void from_json(const nlohmann::json& j, PageView& p) {
    j.at("user_id").get_to(p.user_id);
    j.at("page_url").get_to(p.page_url);
    j.at("timestamp").get_to(p.timestamp);
    if (j.contains("referrer")) {
        j.at("referrer").get_to(p.referrer);
    }
}

// JSON serialization for UserProfile
void to_json(nlohmann::json& j, const UserProfile& u) {
    j = nlohmann::json{
        {"user_id", u.user_id},
        {"name", u.name},
        {"region", u.region},
        {"interests", u.interests},
        {"age", u.age}
    };
}

void from_json(const nlohmann::json& j, UserProfile& u) {
    j.at("user_id").get_to(u.user_id);
    j.at("name").get_to(u.name);
    j.at("region").get_to(u.region);
    if (j.contains("interests")) {
        j.at("interests").get_to(u.interests);
    }
    j.at("age").get_to(u.age);
}

// JSON serialization for EnrichedPageView
void to_json(nlohmann::json& j, const EnrichedPageView& e) {
    j = nlohmann::json{
        {"user_id", e.user_id},
        {"page_url", e.page_url},
        {"timestamp", e.timestamp},
        {"referrer", e.referrer},
        {"user_name", e.user_name},
        {"region", e.region},
        {"age", e.age}
    };
}

int main(int argc, char* argv[]) {
    std::cout << "=== Kawasan Streams Page View Enrichment Example ===\n\n";

    // Parse command line arguments
    std::string broker = "localhost:9092";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--broker" && i + 1 < argc) {
            broker = argv[++i];
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  --broker ADDR    Broker address (default: localhost:9092)\n"
                      << "  --help           Show this help\n";
            return 0;
        }
    }

    std::cout << "Configuration:\n"
              << "  Broker: " << broker << "\n"
              << "  Input topics: page-views, users\n"
              << "  Output topic: pageviews-by-region\n\n";

    // Build the topology
    StreamsBuilder builder;

    // Read page views as stream (userId -> PageView)
    auto pageViews = builder.stream<std::string, std::string>("page-views");

    // Read user profiles as table (userId -> UserProfile)
    auto users = builder.table<std::string, std::string>("users");

    // Join page views with user profiles
    auto enriched = pageViews.template join<std::string, std::string>(
        users,
        [](const std::string& pageViewJson, const std::string& userJson) {
            // In a real implementation, deserialize and join
            return pageViewJson + "|" + userJson;
        }
    );

    // Build the topology
    Topology topology = builder.build();

    // Print topology description
    std::cout << "Topology:\n";
    std::cout << "=========\n";
    std::cout << topology.describe() << "\n";

    // Demonstrate the processing logic with sample data
    std::cout << "Sample Processing:\n";
    std::cout << "==================\n";

    // Sample user profiles (table data)
    std::map<std::string, UserProfile> userProfiles = {
        {"user1", {"user1", "Alice", "US-West", "technology,sports", 28}},
        {"user2", {"user2", "Bob", "US-East", "music,travel", 35}},
        {"user3", {"user3", "Carol", "EU-West", "food,fashion", 24}},
        {"user4", {"user4", "David", "Asia-Pacific", "gaming,movies", 31}},
        {"user5", {"user5", "Eve", "US-West", "books,art", 42}}
    };

    // Sample page view events (stream data)
    std::vector<PageView> pageViewEvents = {
        {"user1", "/products/laptop", 1700000001000, "google.com"},
        {"user2", "/products/headphones", 1700000002000, "facebook.com"},
        {"user1", "/checkout", 1700000003000, "/products/laptop"},
        {"user3", "/products/dress", 1700000004000, "instagram.com"},
        {"user4", "/products/game", 1700000005000, "twitch.tv"},
        {"user1", "/order-confirmation", 1700000006000, "/checkout"},
        {"user5", "/products/book", 1700000007000, "amazon.com"},
        {"user2", "/products/speaker", 1700000008000, "/products/headphones"},
        {"user3", "/products/shoes", 1700000009000, "/products/dress"},
        {"user4", "/products/console", 1700000010000, "/products/game"}
    };

    std::cout << "User Profiles Table:\n";
    std::cout << "--------------------\n";
    for (const auto& [id, profile] : userProfiles) {
        std::cout << "  " << id << " -> " << profile.name
                  << " (region: " << profile.region << ", age: " << profile.age << ")\n";
    }
    std::cout << "\n";

    std::cout << "Page View Events:\n";
    std::cout << "-----------------\n";

    // Enrich page views and aggregate by region
    std::map<std::string, int64_t> viewsByRegion;
    std::map<std::string, std::map<std::string, int64_t>> viewsByRegionAndPage;

    for (const auto& pv : pageViewEvents) {
        auto it = userProfiles.find(pv.user_id);
        if (it != userProfiles.end()) {
            const auto& user = it->second;

            EnrichedPageView enriched{
                pv.user_id,
                pv.page_url,
                pv.timestamp,
                pv.referrer,
                user.name,
                user.region,
                user.age
            };

            std::cout << "  [" << enriched.timestamp << "] "
                      << enriched.user_name << " (" << enriched.region << ") "
                      << "visited " << enriched.page_url << "\n";

            // Aggregate by region
            viewsByRegion[enriched.region]++;
            viewsByRegionAndPage[enriched.region][enriched.page_url]++;
        } else {
            std::cout << "  [" << pv.timestamp << "] "
                      << pv.user_id << " (unknown user) "
                      << "visited " << pv.page_url << "\n";
        }
    }

    std::cout << "\nPage Views by Region:\n";
    std::cout << "---------------------\n";
    for (const auto& [region, count] : viewsByRegion) {
        std::cout << "  " << region << ": " << count << " views\n";
    }

    std::cout << "\nDetailed Page Views by Region and Page:\n";
    std::cout << "---------------------------------------\n";
    for (const auto& [region, pages] : viewsByRegionAndPage) {
        std::cout << "  " << region << ":\n";
        for (const auto& [page, count] : pages) {
            std::cout << "    " << page << ": " << count << "\n";
        }
    }

    std::cout << "\n=== Page View Enrichment Example Complete ===\n";

    return 0;
}
