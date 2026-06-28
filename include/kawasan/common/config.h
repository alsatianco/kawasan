#pragma once

#include <nlohmann/json.hpp>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
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
        return coerce<T>(it->second);
    }

    template <typename T>
    T get(const std::string& key, const T& default_value) const {
        auto it = values_.find(key);
        if (it == values_.end()) {
            return default_value;
        }
        return coerce<T>(it->second);
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
    // Coerce a stored JSON value to T. Crucially, numeric/bool config values
    // supplied via environment-variable substitution (e.g. "${VAR:1}") arrive as
    // JSON *strings* — and so do all values from key=value property files — so a
    // strict get<int> would throw type_error 302. This coerces string→number/bool
    // so env-substituted and key=value configs work uniformly. For already-typed
    // values it behaves exactly like nlohmann's get<T>().
    template <typename T>
    static T coerce(const nlohmann::json& j) {
        if constexpr (std::is_same_v<T, bool>) {
            if (j.is_boolean()) return j.get<bool>();
            if (j.is_number()) return j.get<int64_t>() != 0;
            if (j.is_string()) {
                const std::string s = j.get<std::string>();
                return s == "true" || s == "TRUE" || s == "True" || s == "1";
            }
            return j.get<T>();
        } else if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
            if (j.is_number()) return j.get<T>();
            if (j.is_boolean()) return static_cast<T>(j.get<bool>() ? 1 : 0);
            if (j.is_string()) {
                const std::string s = j.get<std::string>();
                try {
                    if constexpr (std::is_floating_point_v<T>) {
                        return static_cast<T>(std::stod(s));
                    } else {
                        return static_cast<T>(std::stoll(s));
                    }
                } catch (const std::exception&) {
                    throw std::runtime_error("Configuration value '" + s +
                                             "' is not a valid number");
                }
            }
            return j.get<T>();
        } else {
            return j.get<T>();  // strings and other types: no coercion
        }
    }

    std::unordered_map<std::string, nlohmann::json> values_;
};

}  // namespace kawasan

