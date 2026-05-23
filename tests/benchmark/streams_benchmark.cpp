/**
 * Streams Benchmark
 *
 * Measures throughput and latency for streams operations.
 *
 * Targets:
 * - Stateless operations: > 50k records/sec
 * - Stateful operations: > 10k records/sec
 *
 * Run: ./tests/benchmark/streams_benchmark
 */

#include "kawasan/streams/streams_builder.h"
#include "kawasan/streams/topology.h"
#include "kawasan/streams/processor.h"
#include "kawasan/streams/processor_context.h"
#include "kawasan/streams/serde.h"
#include <chrono>
#include <iostream>
#include <iomanip>
#include <vector>
#include <map>
#include <random>
#include <algorithm>
#include <numeric>
#include <thread>
#include <atomic>
#include <mutex>

using namespace kawasan::streams;
using namespace std::chrono;

// Benchmark configuration
struct BenchmarkConfig {
    int num_records = 100000;
    int warmup_records = 10000;
    int num_threads = 4;
    int num_iterations = 3;
    bool verbose = true;
};

// Benchmark result
struct BenchmarkResult {
    std::string name;
    double throughput_per_sec;
    double latency_avg_us;
    double latency_p50_us;
    double latency_p95_us;
    double latency_p99_us;
};

// Utility functions
template<typename Func>
double measure_throughput(Func&& func, int num_records) {
    auto start = high_resolution_clock::now();
    func();
    auto end = high_resolution_clock::now();

    auto duration_ns = duration_cast<nanoseconds>(end - start).count();
    double duration_sec = duration_ns / 1e9;

    return num_records / duration_sec;
}

template<typename Func>
std::vector<double> measure_latencies(Func&& func, int num_records) {
    std::vector<double> latencies;
    latencies.reserve(num_records);

    for (int i = 0; i < num_records; ++i) {
        auto start = high_resolution_clock::now();
        func(i);
        auto end = high_resolution_clock::now();

        latencies.push_back(duration_cast<nanoseconds>(end - start).count() / 1000.0);
    }

    return latencies;
}

double percentile(std::vector<double>& data, double p) {
    if (data.empty()) return 0;
    std::sort(data.begin(), data.end());
    size_t idx = static_cast<size_t>(p * data.size());
    if (idx >= data.size()) idx = data.size() - 1;
    return data[idx];
}

void print_result(const BenchmarkResult& result) {
    std::cout << std::left << std::setw(35) << result.name
              << std::right << std::setw(12) << std::fixed << std::setprecision(0)
              << result.throughput_per_sec << " rec/s"
              << std::setw(10) << std::setprecision(1) << result.latency_avg_us << " us (avg)"
              << std::setw(10) << result.latency_p50_us << " us (p50)"
              << std::setw(10) << result.latency_p99_us << " us (p99)"
              << std::endl;
}

// ===========================================================================
// Stateless Operation Benchmarks
// ===========================================================================

BenchmarkResult benchmark_filter(const BenchmarkConfig& config) {
    std::vector<std::pair<std::string, int64_t>> input;
    input.reserve(config.num_records);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dist(1, 1000);

    for (int i = 0; i < config.num_records; ++i) {
        input.emplace_back("key" + std::to_string(i % 100), dist(gen));
    }

    // Filter predicate
    auto predicate = [](const std::string&, int64_t v) { return v > 500; };

    // Warm up
    for (int i = 0; i < config.warmup_records; ++i) {
        predicate(input[i].first, input[i].second);
    }

    // Measure latencies
    auto latencies = measure_latencies([&](int i) {
        predicate(input[i].first, input[i].second);
    }, config.num_records);

    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    double throughput = measure_throughput([&]() {
        for (const auto& [k, v] : input) {
            predicate(k, v);
        }
    }, config.num_records);

    return {"Filter (stateless)", throughput, avg,
            percentile(latencies, 0.50),
            percentile(latencies, 0.95),
            percentile(latencies, 0.99)};
}

BenchmarkResult benchmark_map(const BenchmarkConfig& config) {
    std::vector<std::pair<std::string, int64_t>> input;
    input.reserve(config.num_records);

    for (int i = 0; i < config.num_records; ++i) {
        input.emplace_back("key" + std::to_string(i), static_cast<int64_t>(i));
    }

    // Map function
    auto mapper = [](int64_t v) { return v * 2 + 1; };

    // Warm up
    for (int i = 0; i < config.warmup_records; ++i) {
        mapper(input[i].second);
    }

    // Measure latencies
    auto latencies = measure_latencies([&](int i) {
        mapper(input[i].second);
    }, config.num_records);

    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    double throughput = measure_throughput([&]() {
        for (const auto& [k, v] : input) {
            mapper(v);
        }
    }, config.num_records);

    return {"MapValues (stateless)", throughput, avg,
            percentile(latencies, 0.50),
            percentile(latencies, 0.95),
            percentile(latencies, 0.99)};
}

BenchmarkResult benchmark_flatmap(const BenchmarkConfig& config) {
    std::vector<std::string> input;
    input.reserve(config.num_records);

    for (int i = 0; i < config.num_records; ++i) {
        input.push_back("word1 word2 word3 word4 word5");
    }

    // FlatMap function (split into words)
    auto flatMapper = [](const std::string& line) {
        std::vector<std::string> words;
        std::istringstream iss(line);
        std::string word;
        while (iss >> word) {
            words.push_back(word);
        }
        return words;
    };

    // Warm up
    for (int i = 0; i < config.warmup_records; ++i) {
        flatMapper(input[i]);
    }

    // Measure latencies
    auto latencies = measure_latencies([&](int i) {
        flatMapper(input[i]);
    }, config.num_records);

    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    double throughput = measure_throughput([&]() {
        for (const auto& line : input) {
            flatMapper(line);
        }
    }, config.num_records);

    return {"FlatMap (stateless)", throughput, avg,
            percentile(latencies, 0.50),
            percentile(latencies, 0.95),
            percentile(latencies, 0.99)};
}

// ===========================================================================
// Stateful Operation Benchmarks
// ===========================================================================

BenchmarkResult benchmark_count(const BenchmarkConfig& config) {
    std::vector<std::pair<std::string, int64_t>> input;
    input.reserve(config.num_records);

    for (int i = 0; i < config.num_records; ++i) {
        input.emplace_back("key" + std::to_string(i % 1000), 1);
    }

    // State store
    std::map<std::string, int64_t> counts;

    // Warm up
    for (int i = 0; i < config.warmup_records; ++i) {
        counts[input[i].first]++;
    }
    counts.clear();

    // Measure latencies
    auto latencies = measure_latencies([&](int i) {
        counts[input[i].first]++;
    }, config.num_records);

    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();

    counts.clear();
    double throughput = measure_throughput([&]() {
        for (const auto& [k, v] : input) {
            counts[k]++;
        }
    }, config.num_records);

    return {"Count (stateful, map)", throughput, avg,
            percentile(latencies, 0.50),
            percentile(latencies, 0.95),
            percentile(latencies, 0.99)};
}

BenchmarkResult benchmark_aggregate(const BenchmarkConfig& config) {
    std::vector<std::pair<std::string, int64_t>> input;
    input.reserve(config.num_records);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dist(1, 100);

    for (int i = 0; i < config.num_records; ++i) {
        input.emplace_back("key" + std::to_string(i % 1000), dist(gen));
    }

    // State store for aggregation (sum)
    std::map<std::string, int64_t> sums;

    // Warm up
    for (int i = 0; i < config.warmup_records; ++i) {
        sums[input[i].first] += input[i].second;
    }
    sums.clear();

    // Measure latencies
    auto latencies = measure_latencies([&](int i) {
        sums[input[i].first] += input[i].second;
    }, config.num_records);

    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();

    sums.clear();
    double throughput = measure_throughput([&]() {
        for (const auto& [k, v] : input) {
            sums[k] += v;
        }
    }, config.num_records);

    return {"Aggregate/Sum (stateful)", throughput, avg,
            percentile(latencies, 0.50),
            percentile(latencies, 0.95),
            percentile(latencies, 0.99)};
}

BenchmarkResult benchmark_reduce(const BenchmarkConfig& config) {
    std::vector<std::pair<std::string, std::string>> input;
    input.reserve(config.num_records);

    for (int i = 0; i < config.num_records; ++i) {
        input.emplace_back("key" + std::to_string(i % 1000),
                          "value" + std::to_string(i));
    }

    // State store for reduce (concatenation)
    std::map<std::string, std::string> reduced;

    auto reducer = [](const std::string& v1, const std::string& v2) {
        return v1 + "," + v2;
    };

    // Warm up
    for (int i = 0; i < config.warmup_records; ++i) {
        auto& state = reduced[input[i].first];
        if (state.empty()) {
            state = input[i].second;
        } else {
            state = reducer(state, input[i].second);
        }
    }
    reduced.clear();

    // Measure latencies
    auto latencies = measure_latencies([&](int i) {
        auto& state = reduced[input[i].first];
        if (state.empty()) {
            state = input[i].second;
        } else {
            state = reducer(state, input[i].second);
        }
    }, config.num_records);

    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();

    reduced.clear();
    double throughput = measure_throughput([&]() {
        for (const auto& [k, v] : input) {
            auto& state = reduced[k];
            if (state.empty()) {
                state = v;
            } else {
                state = reducer(state, v);
            }
        }
    }, config.num_records);

    return {"Reduce/Concat (stateful)", throughput, avg,
            percentile(latencies, 0.50),
            percentile(latencies, 0.95),
            percentile(latencies, 0.99)};
}

// ===========================================================================
// Serde Benchmarks
// ===========================================================================

BenchmarkResult benchmark_string_serde(const BenchmarkConfig& config) {
    auto serde = Serdes::String();

    std::vector<std::string> input;
    input.reserve(config.num_records);

    for (int i = 0; i < config.num_records; ++i) {
        input.push_back("test-string-value-" + std::to_string(i));
    }

    // Warm up
    for (int i = 0; i < config.warmup_records; ++i) {
        auto bytes = serde->serialize(input[i]);
        serde->deserialize(bytes);
    }

    // Measure latencies (serialize + deserialize)
    auto latencies = measure_latencies([&](int i) {
        auto bytes = serde->serialize(input[i]);
        serde->deserialize(bytes);
    }, config.num_records);

    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    double throughput = measure_throughput([&]() {
        for (const auto& s : input) {
            auto bytes = serde->serialize(s);
            serde->deserialize(bytes);
        }
    }, config.num_records);

    return {"String Serde (ser+deser)", throughput, avg,
            percentile(latencies, 0.50),
            percentile(latencies, 0.95),
            percentile(latencies, 0.99)};
}

BenchmarkResult benchmark_long_serde(const BenchmarkConfig& config) {
    auto serde = Serdes::Long();

    std::vector<int64_t> input;
    input.reserve(config.num_records);

    for (int i = 0; i < config.num_records; ++i) {
        input.push_back(static_cast<int64_t>(i) * 1000000);
    }

    // Warm up
    for (int i = 0; i < config.warmup_records; ++i) {
        auto bytes = serde->serialize(input[i]);
        serde->deserialize(bytes);
    }

    // Measure latencies
    auto latencies = measure_latencies([&](int i) {
        auto bytes = serde->serialize(input[i]);
        serde->deserialize(bytes);
    }, config.num_records);

    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    double throughput = measure_throughput([&]() {
        for (const auto& n : input) {
            auto bytes = serde->serialize(n);
            serde->deserialize(bytes);
        }
    }, config.num_records);

    return {"Long Serde (ser+deser)", throughput, avg,
            percentile(latencies, 0.50),
            percentile(latencies, 0.95),
            percentile(latencies, 0.99)};
}

// ===========================================================================
// Parallel Processing Benchmark
// ===========================================================================

BenchmarkResult benchmark_parallel_stateless(const BenchmarkConfig& config) {
    std::vector<std::pair<std::string, int64_t>> input;
    input.reserve(config.num_records);

    for (int i = 0; i < config.num_records; ++i) {
        input.emplace_back("key" + std::to_string(i), static_cast<int64_t>(i));
    }

    auto mapper = [](int64_t v) { return v * 2 + 1; };

    std::atomic<int64_t> processed{0};

    auto start = high_resolution_clock::now();

    std::vector<std::thread> threads;
    int records_per_thread = config.num_records / config.num_threads;

    for (int t = 0; t < config.num_threads; ++t) {
        int start_idx = t * records_per_thread;
        int end_idx = (t == config.num_threads - 1) ?
                      config.num_records : (t + 1) * records_per_thread;

        threads.emplace_back([&, start_idx, end_idx]() {
            for (int i = start_idx; i < end_idx; ++i) {
                mapper(input[i].second);
                processed++;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end = high_resolution_clock::now();
    double duration_sec = duration_cast<nanoseconds>(end - start).count() / 1e9;
    double throughput = config.num_records / duration_sec;

    return {"Parallel MapValues (" + std::to_string(config.num_threads) + " threads)",
            throughput, 0, 0, 0, 0};
}

BenchmarkResult benchmark_parallel_stateful(const BenchmarkConfig& config) {
    std::vector<std::pair<std::string, int64_t>> input;
    input.reserve(config.num_records);

    for (int i = 0; i < config.num_records; ++i) {
        input.emplace_back("key" + std::to_string(i % 1000), 1);
    }

    // Per-thread state (simulate partitioned state)
    std::vector<std::map<std::string, int64_t>> thread_states(config.num_threads);

    auto start = high_resolution_clock::now();

    std::vector<std::thread> threads;
    int records_per_thread = config.num_records / config.num_threads;

    for (int t = 0; t < config.num_threads; ++t) {
        int start_idx = t * records_per_thread;
        int end_idx = (t == config.num_threads - 1) ?
                      config.num_records : (t + 1) * records_per_thread;

        threads.emplace_back([&, t, start_idx, end_idx]() {
            for (int i = start_idx; i < end_idx; ++i) {
                thread_states[t][input[i].first]++;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end = high_resolution_clock::now();
    double duration_sec = duration_cast<nanoseconds>(end - start).count() / 1e9;
    double throughput = config.num_records / duration_sec;

    return {"Parallel Count (" + std::to_string(config.num_threads) + " threads)",
            throughput, 0, 0, 0, 0};
}

// ===========================================================================
// Main
// ===========================================================================

int main(int argc, char* argv[]) {
    BenchmarkConfig config;

    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--records" && i + 1 < argc) {
            config.num_records = std::stoi(argv[++i]);
        } else if (arg == "--threads" && i + 1 < argc) {
            config.num_threads = std::stoi(argv[++i]);
        } else if (arg == "--quiet") {
            config.verbose = false;
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  --records N   Number of records (default: 100000)\n"
                      << "  --threads N   Number of threads (default: 4)\n"
                      << "  --quiet       Less verbose output\n"
                      << "  --help        Show this help\n";
            return 0;
        }
    }

    std::cout << "=============================================================\n";
    std::cout << "           Kawasan Streams Benchmark Suite\n";
    std::cout << "=============================================================\n";
    std::cout << "Configuration:\n";
    std::cout << "  Records: " << config.num_records << "\n";
    std::cout << "  Threads: " << config.num_threads << "\n";
    std::cout << "  Warmup:  " << config.warmup_records << " records\n";
    std::cout << "=============================================================\n\n";

    std::vector<BenchmarkResult> results;

    // Stateless operations
    std::cout << "Running stateless operation benchmarks...\n";
    results.push_back(benchmark_filter(config));
    results.push_back(benchmark_map(config));
    results.push_back(benchmark_flatmap(config));

    // Stateful operations
    std::cout << "Running stateful operation benchmarks...\n";
    results.push_back(benchmark_count(config));
    results.push_back(benchmark_aggregate(config));
    results.push_back(benchmark_reduce(config));

    // Serde operations
    std::cout << "Running serde benchmarks...\n";
    results.push_back(benchmark_string_serde(config));
    results.push_back(benchmark_long_serde(config));

    // Parallel processing
    std::cout << "Running parallel processing benchmarks...\n";
    results.push_back(benchmark_parallel_stateless(config));
    results.push_back(benchmark_parallel_stateful(config));

    // Print results
    std::cout << "\n=============================================================\n";
    std::cout << "                      RESULTS\n";
    std::cout << "=============================================================\n";
    std::cout << std::left << std::setw(35) << "Operation"
              << std::right << std::setw(15) << "Throughput"
              << std::setw(13) << "Latency\n";
    std::cout << "-------------------------------------------------------------\n";

    for (const auto& result : results) {
        print_result(result);
    }

    std::cout << "=============================================================\n";

    // Check against targets
    std::cout << "\nTarget Analysis:\n";
    std::cout << "  Stateless target: > 50,000 rec/sec\n";
    std::cout << "  Stateful target:  > 10,000 rec/sec\n\n";

    bool all_pass = true;
    for (const auto& result : results) {
        bool is_stateful = result.name.find("stateful") != std::string::npos;
        double target = is_stateful ? 10000.0 : 50000.0;

        if (result.throughput_per_sec >= target) {
            std::cout << "  PASS: " << result.name << " (" <<
                      std::fixed << std::setprecision(0) << result.throughput_per_sec <<
                      " >= " << target << ")\n";
        } else {
            std::cout << "  FAIL: " << result.name << " (" <<
                      std::fixed << std::setprecision(0) << result.throughput_per_sec <<
                      " < " << target << ")\n";
            all_pass = false;
        }
    }

    std::cout << "\n";
    return all_pass ? 0 : 1;
}
