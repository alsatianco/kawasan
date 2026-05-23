# Multi-stage build for Kawasan

# Stage 1: Build stage
FROM ubuntu:22.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libboost-all-dev \
    librocksdb-dev \
    libspdlog-dev \
    nlohmann-json3-dev \
    libgtest-dev \
    libssl-dev \
    zlib1g-dev \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy source code
COPY . .

# Build
RUN mkdir build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    cmake --build . -j$(nproc)

# Stage 2: Runtime stage
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libboost-system1.74.0 \
    libboost-program-options1.74.0 \
    librocksdb6.11 \
    libspdlog1 \
    libssl3 \
    zlib1g \
    libsnappy1v5 \
    liblz4-1 \
    libzstd1 \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Create kawasan user and group
RUN groupadd -r kawasan && useradd -r -g kawasan -d /var/lib/kawasan -s /sbin/nologin kawasan

# Copy built binaries
COPY --from=builder /build/build/tools/kawasan-broker /usr/local/bin/
COPY --from=builder /build/config/server.properties.example /etc/kawasan/server.properties

# Create data and log directories with correct permissions
RUN mkdir -p /var/lib/kawasan/data /var/log/kawasan && \
    chown -R kawasan:kawasan /var/lib/kawasan /var/log/kawasan /etc/kawasan

# Update config to use correct paths
RUN sed -i 's|log.dirs=/tmp/kawasan-logs|log.dirs=/var/lib/kawasan/data|' /etc/kawasan/server.properties

# Switch to kawasan user
USER kawasan

# Expose Kafka port and Raft port
EXPOSE 9092 9093

# Health check - verify broker is listening on port 9092
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD nc -z localhost 9092 || exit 1

# Set default command
CMD ["kawasan-broker", "--config", "/etc/kawasan/server.properties"]

