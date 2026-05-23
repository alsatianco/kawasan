#include "kawasan/common/config.h"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <regex>
#include <stdexcept>

namespace kawasan {

// Helper function to expand environment variables in strings
// Supports ${VAR} and ${VAR:default} syntax
std::string expandEnvironmentVariables(const std::string& input) {
    static std::regex env_var_regex(R"(\$\{([^}:]+)(?::([^}]*))?\})");
    std::string result = input;
    std::smatch match;
    
    std::string::const_iterator search_start(result.cbegin());
    while (std::regex_search(search_start, result.cend(), match, env_var_regex)) {
        std::string var_name = match[1].str();
        std::string default_value = match[2].str();
        
        // Get environment variable value
        const char* env_value = std::getenv(var_name.c_str());
        std::string replacement;
        
        if (env_value != nullptr && env_value[0] != '\0') {
            replacement = env_value;
        } else if (match[2].matched) {
            // Use default value if provided
            replacement = default_value;
        } else {
            // No default and no env var - throw error for required variables
            throw std::runtime_error("Required environment variable not set: " + var_name);
        }
        
        // Replace the matched pattern
        size_t pos = match.position(0) + std::distance(result.cbegin(), search_start);
        result.replace(pos, match.length(0), replacement);
        search_start = result.cbegin() + pos + replacement.length();
    }
    
    return result;
}

Config::Config(const std::string& file_path) {
    load(file_path);
}

std::optional<std::string> Config::getString(const std::string& key) const {
    auto it = values_.find(key);
    if (it == values_.end()) {
        return std::nullopt;
    }
    return it->second.get<std::string>();
}

std::optional<int32_t> Config::getInt(const std::string& key) const {
    auto it = values_.find(key);
    if (it == values_.end()) {
        return std::nullopt;
    }
    return it->second.get<int32_t>();
}

std::optional<int64_t> Config::getLong(const std::string& key) const {
    auto it = values_.find(key);
    if (it == values_.end()) {
        return std::nullopt;
    }
    return it->second.get<int64_t>();
}

std::optional<bool> Config::getBool(const std::string& key) const {
    auto it = values_.find(key);
    if (it == values_.end()) {
        return std::nullopt;
    }
    return it->second.get<bool>();
}

void Config::set(const std::string& key, const nlohmann::json& value) {
    values_[key] = value;
}

void Config::setString(const std::string& key, const std::string& value) {
    values_[key] = value;
}

void Config::setInt(const std::string& key, int32_t value) {
    values_[key] = value;
}

void Config::setLong(const std::string& key, int64_t value) {
    values_[key] = value;
}

void Config::setBool(const std::string& key, bool value) {
    values_[key] = value;
}

bool Config::has(const std::string& key) const {
    return values_.find(key) != values_.end();
}

namespace {

// 0A.14: coerce a properties-style string value into a typed JSON value so
// downstream getInt/getBool/getString round-trips behave identically to JSON
// input. Order matters: bool → integer → string fallback.
nlohmann::json coercePropertyValue(const std::string& raw) {
    if (raw == "true") return nlohmann::json(true);
    if (raw == "false") return nlohmann::json(false);
    // Integer detection: optional leading '-', then digits.
    if (!raw.empty()) {
        size_t i = (raw[0] == '-') ? 1 : 0;
        bool all_digits = (i < raw.size());
        for (; i < raw.size(); ++i) {
            if (raw[i] < '0' || raw[i] > '9') { all_digits = false; break; }
        }
        if (all_digits) {
            try {
                long long v = std::stoll(raw);
                return nlohmann::json(static_cast<int64_t>(v));
            } catch (...) {
                // overflow → fall through to string
            }
        }
    }
    return nlohmann::json(raw);
}

void parsePropertiesInto(std::istream& in,
                         std::unordered_map<std::string, nlohmann::json>& out) {
    std::string line;
    while (std::getline(in, line)) {
        // Strip leading/trailing whitespace.
        auto first = line.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) continue;
        if (line[first] == '#' || line[first] == '!') continue;  // comment
        auto last = line.find_last_not_of(" \t\r\n");
        line = line.substr(first, last - first + 1);
        // Split on '=' (or first ':').
        size_t eq = line.find('=');
        if (eq == std::string::npos) {
            eq = line.find(':');
        }
        if (eq == std::string::npos) {
            // Skip malformed line silently — matches Kafka's tolerance.
            continue;
        }
        std::string key = line.substr(0, eq);
        std::string value = line.substr(eq + 1);
        // Trim key/value.
        auto k_first = key.find_first_not_of(" \t");
        auto k_last = key.find_last_not_of(" \t");
        if (k_first == std::string::npos) continue;
        key = key.substr(k_first, k_last - k_first + 1);
        auto v_first = value.find_first_not_of(" \t");
        if (v_first != std::string::npos) {
            auto v_last = value.find_last_not_of(" \t");
            value = value.substr(v_first, v_last - v_first + 1);
        } else {
            value.clear();
        }
        try {
            out[key] = coercePropertyValue(expandEnvironmentVariables(value));
        } catch (const std::runtime_error& e) {
            throw std::runtime_error("Error in config key '" + key + "': " + e.what());
        }
    }
}

}  // namespace

void Config::load(const std::string& file_path) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open configuration file: " + file_path);
    }

    // 0A.14: dispatch by first non-whitespace byte. '{' or '[' → JSON; else
    // Kafka-style key=value properties. Removes the JSON-vs-.properties
    // mismatch that broke the Docker entrypoint.
    char first_char = 0;
    while (file.get(first_char)) {
        if (first_char == ' ' || first_char == '\t' || first_char == '\r' ||
            first_char == '\n') {
            continue;
        }
        file.unget();
        break;
    }
    if (!file) {
        // Empty file is allowed — leaves Config empty.
        return;
    }

    if (first_char == '{' || first_char == '[') {
        nlohmann::json json;
        file >> json;
        for (auto& [key, value] : json.items()) {
            if (value.is_string()) {
                std::string str_value = value.get<std::string>();
                try {
                    values_[key] = expandEnvironmentVariables(str_value);
                } catch (const std::runtime_error& e) {
                    throw std::runtime_error("Error in config key '" + key + "': " + e.what());
                }
            } else {
                values_[key] = value;
            }
        }
    } else {
        parsePropertiesInto(file, values_);
    }
}

void Config::save(const std::string& file_path) const {
    std::ofstream file(file_path);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open configuration file: " + file_path);
    }

    nlohmann::json json(values_);
    file << json.dump(2);
}

void Config::loadFromJson(const std::string& json_str) {
    nlohmann::json json = nlohmann::json::parse(json_str);
    for (auto& [key, value] : json.items()) {
        values_[key] = value;
    }
}

std::string Config::toJson() const {
    nlohmann::json json(values_);
    return json.dump(2);
}

void Config::merge(const Config& other) {
    for (const auto& [key, value] : other.values_) {
        values_[key] = value;
    }
}

std::vector<std::string> Config::keys() const {
    std::vector<std::string> result;
    result.reserve(values_.size());
    for (const auto& [key, _] : values_) {
        result.push_back(key);
    }
    return result;
}

void Config::validate() const {
    std::vector<std::string> errors;
    std::vector<std::string> warnings;

    // Required keys
    const std::vector<std::string> required_keys = {
        "broker.id",
        "host", 
        "port",
        "log.dirs"
    };

    for (const auto& key : required_keys) {
        if (!has(key)) {
            errors.push_back("Missing required configuration: " + key);
        }
    }

    // Validate broker.id
    if (has("broker.id")) {
        auto broker_id = getInt("broker.id");
        if (broker_id && *broker_id < 0) {
            errors.push_back("broker.id must be non-negative");
        }
    }

    // Validate port
    if (has("port")) {
        auto port = getInt("port");
        if (port && (*port < 1 || *port > 65535)) {
            errors.push_back("port must be between 1 and 65535");
        }
    }

    // Validate num.network.threads
    if (has("num.network.threads")) {
        auto threads = getInt("num.network.threads");
        if (threads && *threads < 1) {
            errors.push_back("num.network.threads must be at least 1");
        }
        if (threads && *threads > 1024) {
            warnings.push_back("num.network.threads is very high (" + 
                             std::to_string(*threads) + "), may cause resource issues");
        }
    }

    // Validate num.io.threads
    if (has("num.io.threads")) {
        auto threads = getInt("num.io.threads");
        if (threads && *threads < 1) {
            errors.push_back("num.io.threads must be at least 1");
        }
    }

    // Validate log.segment.bytes
    if (has("log.segment.bytes")) {
        auto segment_bytes = getLong("log.segment.bytes");
        if (segment_bytes && *segment_bytes < 1024) {
            errors.push_back("log.segment.bytes must be at least 1024 bytes");
        }
    }

    // Validate log.retention.hours
    if (has("log.retention.hours")) {
        auto retention = getLong("log.retention.hours");
        if (retention && *retention < -1) {
            errors.push_back("log.retention.hours must be -1 (unlimited) or positive");
        }
    }

    // Validate replication factor
    if (has("default.replication.factor")) {
        auto rf = getInt("default.replication.factor");
        if (rf && *rf < 1) {
            errors.push_back("default.replication.factor must be at least 1");
        }
    }

    // Validate min.insync.replicas
    if (has("min.insync.replicas")) {
        auto min_isr = getInt("min.insync.replicas");
        if (min_isr && *min_isr < 1) {
            errors.push_back("min.insync.replicas must be at least 1");
        }
        
        // Check min.insync.replicas <= replication.factor
        if (has("default.replication.factor")) {
            auto rf = getInt("default.replication.factor");
            if (min_isr && rf && *min_isr > *rf) {
                errors.push_back("min.insync.replicas (" + std::to_string(*min_isr) + 
                               ") cannot be greater than default.replication.factor (" + 
                               std::to_string(*rf) + ")");
            }
        }
    }

    // Validate monitoring port
    if (has("monitoring.port")) {
        auto mon_port = getInt("monitoring.port");
        if (mon_port && (*mon_port < 1 || *mon_port > 65535)) {
            errors.push_back("monitoring.port must be between 1 and 65535");
        }
        
        // Warn if monitoring port is same as broker port
        if (has("port")) {
            auto broker_port = getInt("port");
            if (mon_port && broker_port && *mon_port == *broker_port) {
                errors.push_back("monitoring.port cannot be the same as broker port");
            }
        }
    }

    // Validate compression type
    if (has("compression.type")) {
        auto compression = getString("compression.type");
        if (compression) {
            const std::vector<std::string> valid_types = {
                "none", "gzip", "snappy", "lz4", "zstd"
            };
            if (std::find(valid_types.begin(), valid_types.end(), *compression) == 
                valid_types.end()) {
                errors.push_back("Invalid compression.type: " + *compression + 
                               ". Valid types: none, gzip, snappy, lz4, zstd");
            }
        }
    }

    // Print warnings
    for (const auto& warning : warnings) {
        // Note: Logger may not be initialized yet, so we use std::cerr
        std::cerr << "WARNING: " << warning << std::endl;
    }

    // Throw if any errors
    if (!errors.empty()) {
        std::string error_msg = "Configuration validation failed:\n";
        for (const auto& error : errors) {
            error_msg += "  - " + error + "\n";
        }
        throw std::runtime_error(error_msg);
    }
}

}  // namespace kawasan

