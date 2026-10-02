#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>

#include "kawasan/common/types.h"

namespace kawasan::broker {

/// @brief An (epoch, end offset) pair as answered by OffsetForLeaderEpoch /
/// Log::epochEndOffset; (-1, -1) is UNDEFINED.
struct EpochAnswer {
    int32_t epoch = -1;
    Offset end_offset = -1;
    bool defined() const { return epoch >= 0 && end_offset >= 0; }
};

/// @brief M8-E3: the offset a follower truncates its log to before replicating
/// from a (new) leader; >= follower_leo means nothing to cut.
///
/// KIP-101: the follower asked the leader where its latest epoch ends
/// (`leader`). If the leader knows that epoch, cut there. If the leader only
/// knows an older epoch E, cut at the earlier of where E ends on the leader
/// and here (`own_end_of_leader_epoch`). Without epoch history on either side
/// (pre-M8 logs, UNDEFINED answer) fall back to the follower's high watermark —
/// everything up to it was committed by the ISR, so the leader has it.
inline Offset followerTruncationOffset(std::optional<int32_t> follower_latest_epoch,
                                       std::optional<EpochAnswer> leader,
                                       EpochAnswer own_end_of_leader_epoch, Offset follower_leo,
                                       Offset follower_hw) {
    if (!follower_latest_epoch || !leader || !leader->defined()) {
        return std::min(follower_hw, follower_leo);
    }
    Offset target = std::min(leader->end_offset, follower_leo);
    if (leader->epoch < *follower_latest_epoch && own_end_of_leader_epoch.defined() &&
        own_end_of_leader_epoch.epoch == leader->epoch) {
        target = std::min(target, own_end_of_leader_epoch.end_offset);
    }
    return target;
}

}  // namespace kawasan::broker
