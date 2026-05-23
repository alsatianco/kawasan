#pragma once

#include <vector>

namespace kawasan::storage {

// Decode an LZ4 frame emitted by Kafka clients. Accepts both standard frames
// and ones with legacy/broken descriptor checksums.
std::vector<uint8_t> decodeKafkaLz4Frame(const std::vector<uint8_t>& payload);

}  // namespace kawasan::storage

