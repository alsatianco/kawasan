#pragma once

#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>

#include <memory>
#include <string>

namespace kawasan {

/// @brief Logging utility wrapper around spdlog
class Logger {
public:
    static void init(const std::string& log_level = "info",
                     const std::string& log_file = "",
                     bool use_json_format = false);

    template <typename... Args>
    static void trace(const char* fmt, Args&&... args) {
        if (logger_) {
            logger_->trace(fmt::runtime(fmt), std::forward<Args>(args)...);
        }
    }

    template <typename... Args>
    static void debug(const char* fmt, Args&&... args) {
        if (logger_) {
            logger_->debug(fmt::runtime(fmt), std::forward<Args>(args)...);
        }
    }

    template <typename... Args>
    static void info(const char* fmt, Args&&... args) {
        if (logger_) {
            logger_->info(fmt::runtime(fmt), std::forward<Args>(args)...);
        }
    }

    template <typename... Args>
    static void warn(const char* fmt, Args&&... args) {
        if (logger_) {
            logger_->warn(fmt::runtime(fmt), std::forward<Args>(args)...);
        }
    }

    template <typename... Args>
    static void error(const char* fmt, Args&&... args) {
        if (logger_) {
            logger_->error(fmt::runtime(fmt), std::forward<Args>(args)...);
        }
    }

    template <typename... Args>
    static void critical(const char* fmt, Args&&... args) {
        if (logger_) {
            logger_->critical(fmt::runtime(fmt), std::forward<Args>(args)...);
        }
    }

    static std::shared_ptr<spdlog::logger> get() { return logger_; }

private:
    static std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace kawasan
