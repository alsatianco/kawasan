#pragma once

#include <exception>
#include <string>

#include "kawasan/common/types.h"

namespace kawasan {

/// @brief Base exception class for Kawasan errors
class KawasanException : public std::exception {
public:
    explicit KawasanException(const std::string& message) : message_(message) {}
    explicit KawasanException(ErrorCode code) : code_(code), message_(toString(code)) {}
    KawasanException(ErrorCode code, const std::string& details)
        : code_(code), message_(toString(code) + ": " + details) {}

    const char* what() const noexcept override { return message_.c_str(); }

    ErrorCode code() const { return code_; }

    static std::string toString(ErrorCode code);

private:
    ErrorCode code_ = ErrorCode::NONE;
    std::string message_;
};

/// @brief Protocol-related errors
class ProtocolException : public KawasanException {
public:
    using KawasanException::KawasanException;
};

/// @brief Storage-related errors
class StorageException : public KawasanException {
public:
    using KawasanException::KawasanException;
};

/// @brief Network-related errors
class NetworkException : public KawasanException {
public:
    using KawasanException::KawasanException;
};

/// @brief Configuration-related errors
class ConfigException : public KawasanException {
public:
    using KawasanException::KawasanException;
};

/// @brief Timeout errors
class TimeoutException : public KawasanException {
public:
    using KawasanException::KawasanException;
};

/// @brief Result type for operations that can fail
template <typename T>
class Result {
public:
    static Result success(T value) {
        Result r;
        r.value_ = std::move(value);
        r.error_code_ = ErrorCode::NONE;
        return r;
    }

    static Result failure(ErrorCode code, const std::string& message = "") {
        Result r;
        r.error_code_ = code;
        r.error_message_ = message;
        return r;
    }

    bool isSuccess() const { return error_code_ == ErrorCode::NONE; }
    bool isFailure() const { return !isSuccess(); }

    const T& value() const {
        if (isFailure()) {
            throw KawasanException(error_code_, error_message_);
        }
        return value_;
    }

    T& value() {
        if (isFailure()) {
            throw KawasanException(error_code_, error_message_);
        }
        return value_;
    }

    ErrorCode errorCode() const { return error_code_; }
    const std::string& errorMessage() const { return error_message_; }

private:
    T value_;
    ErrorCode error_code_ = ErrorCode::NONE;
    std::string error_message_;
};

}  // namespace kawasan

