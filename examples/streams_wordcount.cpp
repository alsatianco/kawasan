/**
 * Streams Word Count Example
 *
 * Classic word count example demonstrating:
 * - Reading from input topic
 * - FlatMap to split lines into words
 * - GroupByKey to group by word
 * - Count to count occurrences
 * - Output to result topic
 *
 * Equivalent Kafka Streams:
 *   KStream<String, String> textLines = builder.stream("text-input");
 *   KTable<String, Long> wordCounts = textLines
 *       .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
 *       .groupBy((key, word) -> word)
 *       .count(Materialized.as("word-counts-store"));
 *   wordCounts.toStream().to("word-counts");
 *
 * Usage:
 *   ./examples/streams_wordcount
 *
 * Prerequisites:
 *   - Kawasan broker running on localhost:9092
 *   - Topics created: text-input, word-counts
 */

#include "kawasan/streams/streams_builder.h"
#include "kawasan/streams/topology.h"
#include "kawasan/streams/kawasan_streams.h"
#include "kawasan/streams/serde.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <cctype>

using namespace kawasan::streams;

int main(int argc, char* argv[]) {
    std::cout << "=== Kawasan Streams Word Count Example ===\n\n";

    // Parse command line arguments
    std::string input_topic = "text-input";
    std::string output_topic = "word-counts";
    std::string broker = "localhost:9092";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--input" && i + 1 < argc) {
            input_topic = argv[++i];
        } else if (arg == "--output" && i + 1 < argc) {
            output_topic = argv[++i];
        } else if (arg == "--broker" && i + 1 < argc) {
            broker = argv[++i];
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  --input TOPIC    Input topic (default: text-input)\n"
                      << "  --output TOPIC   Output topic (default: word-counts)\n"
                      << "  --broker ADDR    Broker address (default: localhost:9092)\n"
                      << "  --help           Show this help\n";
            return 0;
        }
    }

    std::cout << "Configuration:\n"
              << "  Input topic:  " << input_topic << "\n"
              << "  Output topic: " << output_topic << "\n"
              << "  Broker:       " << broker << "\n\n";

    // Build the topology
    StreamsBuilder builder;

    // Read text lines from input topic
    auto textLines = builder.stream<std::string, std::string>(input_topic);

    // Split lines into words (flatMap) and normalize to lowercase
    auto words = textLines.flatMap<std::string, std::string>(
        [](const std::string& /*key*/, const std::string& line) {
            std::vector<std::pair<std::string, std::string>> result;

            // Convert to lowercase and split by whitespace/punctuation
            std::string normalized;
            for (char c : line) {
                if (std::isalnum(static_cast<unsigned char>(c))) {
                    normalized += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
                } else if (!normalized.empty()) {
                    result.emplace_back(normalized, "1");
                    normalized.clear();
                }
            }
            if (!normalized.empty()) {
                result.emplace_back(normalized, "1");
            }

            return result;
        }
    );

    // Group by word (the word is now the key)
    auto groupedByWord = words.groupByKey();

    // Count occurrences with named store
    Materialized<std::string, int64_t> materialized("word-counts-store");
    auto wordCounts = groupedByWord.count(materialized);

    // Convert to stream and output
    // Note: In full implementation, this would write to the output topic
    // wordCounts.toStream().to(output_topic);

    // Build the topology
    Topology topology = builder.build();

    // Print topology description
    std::cout << "Topology:\n";
    std::cout << "=========\n";
    std::cout << topology.describe() << "\n";

    // Demonstrate the processing logic with sample data
    std::cout << "Sample Processing:\n";
    std::cout << "==================\n";

    std::vector<std::string> sampleLines = {
        "Hello world",
        "Hello Kawasan streams",
        "Streams are awesome",
        "Word count is a classic example",
        "Hello hello hello world"
    };

    std::map<std::string, int64_t> counts;

    for (const auto& line : sampleLines) {
        std::cout << "Input: \"" << line << "\"\n";

        // Process line
        std::string normalized;
        for (char c : line) {
            if (std::isalnum(static_cast<unsigned char>(c))) {
                normalized += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            } else if (!normalized.empty()) {
                counts[normalized]++;
                normalized.clear();
            }
        }
        if (!normalized.empty()) {
            counts[normalized]++;
        }
    }

    std::cout << "\nWord Counts:\n";
    std::cout << "------------\n";

    // Sort by count (descending)
    std::vector<std::pair<std::string, int64_t>> sorted_counts(counts.begin(), counts.end());
    std::sort(sorted_counts.begin(), sorted_counts.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    for (const auto& [word, count] : sorted_counts) {
        std::cout << "  " << word << ": " << count << "\n";
    }

    std::cout << "\n=== Word Count Example Complete ===\n";

    // In a real application, you would:
    // StreamsConfig config;
    // config.set("application.id", "wordcount-app");
    // config.set("bootstrap.servers", broker);
    //
    // KawasanStreams streams(builder.build(), config);
    // streams.start();
    // ... wait for shutdown ...
    // streams.close();

    return 0;
}
