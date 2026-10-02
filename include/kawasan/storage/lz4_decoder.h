#pragma once

#include <cstdint>
#include <vector>

namespace kawasan::storage {

// Decode an LZ4 frame emitted by Kafka clients. Accepts both standard frames
// and ones with legacy/broken descriptor checksums.
std::vector<uint8_t> decodeKafkaLz4Frame(const std::vector<uint8_t>& payload);

// Encode an LZ4 frame that Kafka clients accept (standard LZ4 frame format via
// liblz4's LZ4F_compressFrame). Used when re-serializing a batch for storage.
std::vector<uint8_t> encodeKafkaLz4Frame(const std::vector<uint8_t>& payload);

}  // namespace kawasan::storage
