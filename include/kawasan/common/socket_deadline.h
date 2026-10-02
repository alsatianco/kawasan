#pragma once

#include <poll.h>
#include <sys/socket.h>

#include <boost/asio.hpp>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace kawasan {

/// @brief Moves exactly `len` bytes over a NON-blocking socket
/// (`socket.non_blocking(true)`), waiting with poll() but never past `deadline`.
/// Throws std::runtime_error on error, peer close, or timeout. Asio's own
/// synchronous read/write wait without bound, which lets a frozen or
/// black-holed peer wedge the calling thread forever.
inline void transferWithDeadline(boost::asio::ip::tcp::socket& socket, bool writing,
                                 uint8_t* data, size_t len,
                                 std::chrono::steady_clock::time_point deadline) {
    size_t done = 0;
    while (done < len) {
        boost::system::error_code ec;
        const size_t n =
            writing ? socket.write_some(boost::asio::buffer(data + done, len - done), ec)
                    : socket.read_some(boost::asio::buffer(data + done, len - done), ec);
        if (!ec) {
            if (n == 0 && !writing) {
                throw std::runtime_error("connection closed by peer");
            }
            done += n;
            continue;
        }
        if (ec != boost::asio::error::would_block && ec != boost::asio::error::try_again) {
            throw std::runtime_error(std::string(writing ? "write" : "read") +
                                     " error: " + ec.message());
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (remaining.count() <= 0) {
            throw std::runtime_error(std::string(writing ? "write" : "read") + " timed out");
        }
        pollfd pfd{};
        pfd.fd = socket.native_handle();
        pfd.events = writing ? POLLOUT : POLLIN;
        const int rc = ::poll(&pfd, 1, static_cast<int>(remaining.count()));
        if (rc < 0 && errno != EINTR) {
            throw std::runtime_error("poll failed");
        }
    }
}

/// @brief Connects `socket` to `host:port` without blocking past `deadline`:
/// resolves (numeric hosts never block), opens the socket NON-blocking, and
/// waits for the handshake with poll(). Leaves the socket non-blocking for
/// transferWithDeadline. Throws std::runtime_error on failure or timeout. (A
/// frozen peer's full listen backlog makes a plain connect() hang for minutes.)
inline void connectWithDeadline(boost::asio::ip::tcp::socket& socket, const std::string& host,
                                int port, std::chrono::steady_clock::time_point deadline) {
    boost::asio::ip::tcp::resolver resolver(socket.get_executor());
    const auto endpoints = resolver.resolve(host, std::to_string(port));
    std::string last_error = "no endpoints";
    for (const auto& entry : endpoints) {
        boost::system::error_code ec;
        socket.close(ec);
        socket.open(entry.endpoint().protocol(), ec);
        if (ec) {
            last_error = ec.message();
            continue;
        }
        socket.non_blocking(true);
        socket.connect(entry.endpoint(), ec);
        if (ec == boost::asio::error::in_progress || ec == boost::asio::error::would_block) {
            while (true) {
                const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - std::chrono::steady_clock::now());
                if (remaining.count() <= 0) {
                    socket.close(ec);
                    throw std::runtime_error("connect timed out");
                }
                pollfd pfd{};
                pfd.fd = socket.native_handle();
                pfd.events = POLLOUT;
                const int rc = ::poll(&pfd, 1, static_cast<int>(remaining.count()));
                if (rc > 0) {
                    break;
                }
                if (rc < 0 && errno != EINTR) {
                    break;
                }
            }
            int so_error = 0;
            socklen_t len = sizeof(so_error);
            ::getsockopt(socket.native_handle(), SOL_SOCKET, SO_ERROR, &so_error, &len);
            ec = boost::system::error_code(so_error, boost::system::system_category());
        }
        if (!ec) {
            return;
        }
        last_error = ec.message();
    }
    boost::system::error_code ignored;
    socket.close(ignored);
    throw std::runtime_error("connect failed: " + last_error);
}

}  // namespace kawasan
