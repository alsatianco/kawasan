// P3: socket tuning on accepted connections. Kafka brokers disable Nagle
// (TCP_NODELAY) — without it small request/response round-trips can eat a
// delayed-ACK stall — enable keep-alive so half-open peers are detected,
// and allow operator-tuned kernel buffer sizes.
#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "kawasan/broker/network/tcp_server.h"

namespace kawasan::broker::network {

namespace {

using tcp = boost::asio::ip::tcp;

// Loopback socket pair: returns the ACCEPTED (server-side) socket, which is
// the one the broker tunes.
tcp::socket makeAcceptedSocket(boost::asio::io_context& io, tcp::socket& client) {
    tcp::acceptor acceptor(io, tcp::endpoint(boost::asio::ip::address_v4::loopback(), 0));
    client.connect(acceptor.local_endpoint());
    return acceptor.accept();
}

}  // namespace

TEST(TcpSocketTuning, AppliesNoDelayKeepAliveAndBuffers) {
    boost::asio::io_context io;
    tcp::socket client(io);
    tcp::socket accepted = makeAcceptedSocket(io, client);

    SocketTuning tuning;
    tuning.no_delay = true;
    tuning.keep_alive = true;
    // Stay below common kernel caps, which can clamp larger requests.
    tuning.send_buffer_bytes = 65536;
    tuning.recv_buffer_bytes = 65536;
    applySocketTuning(accepted, tuning);

    tcp::no_delay no_delay;
    accepted.get_option(no_delay);
    EXPECT_TRUE(no_delay.value());

    boost::asio::socket_base::keep_alive keep_alive;
    accepted.get_option(keep_alive);
    EXPECT_TRUE(keep_alive.value());

    boost::asio::socket_base::send_buffer_size snd;
    accepted.get_option(snd);
    EXPECT_GE(snd.value(), tuning.send_buffer_bytes) << "SO_SNDBUF must honor the configured size";

    boost::asio::socket_base::receive_buffer_size rcv;
    accepted.get_option(rcv);
    EXPECT_GE(rcv.value(), tuning.recv_buffer_bytes) << "SO_RCVBUF must honor the configured size";
}

TEST(TcpSocketTuning, ZeroBufferSizesKeepOsDefaults) {
    boost::asio::io_context io;
    tcp::socket client(io);
    tcp::socket accepted = makeAcceptedSocket(io, client);

    boost::asio::socket_base::send_buffer_size before;
    accepted.get_option(before);

    SocketTuning tuning;  // buffers default to 0 = leave OS defaults
    applySocketTuning(accepted, tuning);

    boost::asio::socket_base::send_buffer_size after;
    accepted.get_option(after);
    EXPECT_EQ(after.value(), before.value())
        << "buffer sizes must be untouched when not configured";

    tcp::no_delay no_delay;
    accepted.get_option(no_delay);
    EXPECT_TRUE(no_delay.value()) << "no_delay defaults to on";
}

TEST(TcpSocketTuning, TuningCanBeDisabled) {
    boost::asio::io_context io;
    tcp::socket client(io);
    tcp::socket accepted = makeAcceptedSocket(io, client);

    SocketTuning tuning;
    tuning.no_delay = false;
    tuning.keep_alive = false;
    applySocketTuning(accepted, tuning);

    tcp::no_delay no_delay;
    accepted.get_option(no_delay);
    EXPECT_FALSE(no_delay.value());
}

}  // namespace kawasan::broker::network
