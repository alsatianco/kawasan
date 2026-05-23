#pragma once

#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <unordered_map>

namespace kawasan {

/// @brief Configuration management for Kawasan components
class Config {
public:
    Config() = default;
    explicit Config(const std::string& file_path);

    // Get configuration values
    template <typename T>
    T get(const std::string& key) const {
        auto it = values_.find(key);
        if (it == values_.end()) {
            throw std::runtime_error("Configuration key not found: " + key);
        }
        return it->second.get<T>();
    }

    template <typename T>
    T get(const std::string& key, const T& default_value) const {
        auto it = values_.find(key);
        if (it == values_.end()) {
            return default_value;
        }
        return it->second.get<T>();
    }

    std::optional<std::string> getString(const std::string& key) const;
    std::optional<int32_t> getInt(const std::string& key) const;
    std::optional<int64_t> getLong(const std::string& key) const;
    std::optional<bool> getBool(const std::string& key) const;

    // Set configuration values
    void set(const std::string& key, const nlohmann::json& value);
    void setString(const std::string& key, const std::string& value);
    void setInt(const std::string& key, int32_t value);
    void setLong(const std::string& key, int64_t value);
    void setBool(const std::string& key, bool value);

    // Check if key exists
    bool has(const std::string& key) const;

    // Load/save configuration
    void load(const std::string& file_path);
    void save(const std::string& file_path) const;
    void loadFromJson(const std::string& json_str);
    std::string toJson() const;

    // Merge configurations
    void merge(const Config& other);

    // Get all keys
    std::vector<std::string> keys() const;

    // Validate configuration
    void validate() const;

private:
    std::unordered_map<std::string, nlohmann::json> values_;
};

}  // namespace kawasan

