#include "kawasan/common/logger.h"

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace kawasan {

std::shared_ptr<spdlog::logger> Logger::logger_ = nullptr;

void Logger::init(const std::string& log_level, const std::string& log_file, bool use_json_format) {
    std::vector<spdlog::sink_ptr> sinks;

    // Determine log pattern based on format
    std::string pattern;
    if (use_json_format) {
        // JSON format: {"timestamp":"...","level":"...","thread":"...","message":"..."}
        pattern = R"({"timestamp":"%Y-%m-%dT%H:%M:%S.%e","level":"%l","thread":"%t","message":"%v"})";
    } else {
        // Standard format
        pattern = "[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v";
    }

    // Console sink
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    if (use_json_format) {
        // Disable color codes in JSON mode
        console_sink->set_pattern(R"({"timestamp":"%Y-%m-%dT%H:%M:%S.%e","level":"%l","thread":"%t","message":"%v"})");
    } else {
        console_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");
    }
    sinks.push_back(console_sink);

    // File sink if specified
    if (!log_file.empty()) {
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_file, true);
        if (use_json_format) {
            file_sink->set_pattern(R"({"timestamp":"%Y-%m-%dT%H:%M:%S.%e","level":"%l","thread":"%t","message":"%v"})");
        } else {
            file_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%t] %v");
        }
        sinks.push_back(file_sink);
    }

    logger_ = std::make_shared<spdlog::logger>("kawasan", sinks.begin(), sinks.end());

    // Set log level
    if (log_level == "trace") {
        logger_->set_level(spdlog::level::trace);
    } else if (log_level == "debug") {
        logger_->set_level(spdlog::level::debug);
    } else if (log_level == "info") {
        logger_->set_level(spdlog::level::info);
    } else if (log_level == "warn" || log_level == "warning") {
        logger_->set_level(spdlog::level::warn);
    } else if (log_level == "error") {
        logger_->set_level(spdlog::level::err);
    } else if (log_level == "critical") {
        logger_->set_level(spdlog::level::critical);
    } else {
        logger_->set_level(spdlog::level::info);
    }

    logger_->flush_on(spdlog::level::warn);
    spdlog::register_logger(logger_);
}

}  // namespace kawasan

