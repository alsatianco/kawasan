#pragma once

#include <cstdint>

#include "kawasan/common/types.h"

namespace kawasan::broker {

/// @brief Producer ids must be unique across the whole cluster: every broker's
/// idempotence state (rebuilt from replicated logs) sees every producer's
/// batches, so two brokers handing out the same id makes a new producer's
/// first writes look like retries of another's — deduplicated and acked
/// without being written. In a cluster each broker therefore issues ids from
/// its own range: broker id in the high 32 bits, its local durable sequence in
/// the low 32 (2^31 ids per broker; never negative). Single-node keeps plain
/// sequential ids.
inline int64_t clusterProducerId(BrokerId broker_id, int64_t local_sequence) {
    return (static_cast<int64_t>(broker_id) << 32) | (local_sequence & 0xFFFFFFFFLL);
}

}  // namespace kawasan::broker
