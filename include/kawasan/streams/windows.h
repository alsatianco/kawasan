#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace kawasan {
namespace streams {

using Duration = std::chrono::milliseconds;

/**
 * Represents a time window.
 *
 * A window defines a period of time for aggregating records.
 * Each window has a start time (inclusive) and an end time (exclusive).
 */
class Window {
public:
    /**
     * Create a new window.
     *
     * @param start_ms Window start time in milliseconds (inclusive)
     * @param end_ms Window end time in milliseconds (exclusive)
     */
    Window(int64_t start_ms, int64_t end_ms)
        : start_ms_(start_ms), end_ms_(end_ms) {}

    /**
     * Get the start time of this window.
     * @return Start time in milliseconds
     */
    int64_t startTime() const { return start_ms_; }

    /**
     * Get the end time of this window.
     * @return End time in milliseconds
     */
    int64_t endTime() const { return end_ms_; }

    /**
     * Check if this window overlaps with another window.
     */
    bool overlaps(const Window& other) const {
        return start_ms_ < other.end_ms_ && other.start_ms_ < end_ms_;
    }

    /**
     * Check if a timestamp falls within this window.
     *
     * @param timestamp_ms Timestamp in milliseconds
     * @return true if timestamp is within [startTime, endTime)
     */
    bool contains(int64_t timestamp_ms) const {
        return timestamp_ms >= start_ms_ && timestamp_ms < end_ms_;
    }

    bool operator==(const Window& other) const {
        return start_ms_ == other.start_ms_ && end_ms_ == other.end_ms_;
    }

    bool operator<(const Window& other) const {
        if (start_ms_ != other.start_ms_) {
            return start_ms_ < other.start_ms_;
        }
        return end_ms_ < other.end_ms_;
    }

private:
    int64_t start_ms_;
    int64_t end_ms_;
};

/**
 * TimeWindows defines fixed-size, non-overlapping or overlapping (hopping) windows.
 *
 * Tumbling windows: advance = size (non-overlapping)
 * Hopping windows: advance < size (overlapping)
 *
 * Example - tumbling window of 5 seconds:
 *   TimeWindows::of(std::chrono::seconds(5))
 *
 * Example - hopping window of 10 seconds, advance every 5 seconds:
 *   TimeWindows::of(std::chrono::seconds(10)).advanceBy(std::chrono::seconds(5))
 */
class TimeWindows {
public:
    /**
     * Create a tumbling time window of the given size.
     *
     * Tumbling windows have no overlap - the advance interval equals the window size.
     *
     * @param size Window size
     * @return TimeWindows configuration
     */
    static TimeWindows of(Duration size) {
        return TimeWindows(size.count(), size.count());
    }

    /**
     * Set the advance interval for hopping windows.
     *
     * If advance < size, windows will overlap (hopping windows).
     * If advance == size, windows will not overlap (tumbling windows).
     *
     * @param advance The advance interval
     * @return TimeWindows configuration
     */
    TimeWindows advanceBy(Duration advance) {
        advance_ms_ = advance.count();
        return *this;
    }

    /**
     * Set the grace period for late-arriving records.
     *
     * Records arriving after the window closes but within the grace period
     * will still be processed.
     *
     * @param grace Grace period duration
     * @return TimeWindows configuration
     */
    TimeWindows grace(Duration grace) {
        grace_ms_ = grace.count();
        return *this;
    }

    /**
     * Get the window size in milliseconds.
     */
    int64_t sizeMs() const { return size_ms_; }

    /**
     * Get the advance interval in milliseconds.
     */
    int64_t advanceMs() const { return advance_ms_; }

    /**
     * Get the grace period in milliseconds.
     */
    int64_t graceMs() const { return grace_ms_; }

    /**
     * Calculate all windows that the given timestamp belongs to.
     *
     * @param timestamp_ms Record timestamp
     * @return Vector of windows containing this timestamp
     */
    std::vector<Window> windowsForTimestamp(int64_t timestamp_ms) const {
        std::vector<Window> windows;

        // Find the earliest window that could contain this timestamp
        int64_t earliest_start = timestamp_ms - size_ms_ + advance_ms_;
        earliest_start = (earliest_start / advance_ms_) * advance_ms_;
        if (earliest_start < 0) {
            earliest_start = 0;
        }

        // Find all windows containing this timestamp
        for (int64_t start = earliest_start; start <= timestamp_ms; start += advance_ms_) {
            int64_t end = start + size_ms_;
            if (timestamp_ms >= start && timestamp_ms < end) {
                windows.emplace_back(start, end);
            }
        }

        return windows;
    }

private:
    TimeWindows(int64_t size_ms, int64_t advance_ms)
        : size_ms_(size_ms), advance_ms_(advance_ms), grace_ms_(0) {}

    int64_t size_ms_;
    int64_t advance_ms_;
    int64_t grace_ms_;
};

/**
 * SessionWindows defines session windows based on inactivity gaps.
 *
 * Session windows group events that occur close together, separated by
 * periods of inactivity. The window size is dynamic and depends on the
 * timing of the events.
 *
 * Example - sessions with 30 second inactivity gap:
 *   SessionWindows::with(std::chrono::seconds(30))
 */
class SessionWindows {
public:
    /**
     * Create session windows with the given inactivity gap.
     *
     * If no new records arrive for a key within the inactivity gap,
     * the session closes and a new session starts with the next record.
     *
     * @param inactivity_gap Maximum time between events in the same session
     * @return SessionWindows configuration
     */
    static SessionWindows with(Duration inactivity_gap) {
        return SessionWindows(inactivity_gap.count());
    }

    /**
     * Set the grace period for late-arriving records.
     *
     * @param grace Grace period duration
     * @return SessionWindows configuration
     */
    SessionWindows grace(Duration grace) {
        grace_ms_ = grace.count();
        return *this;
    }

    /**
     * Get the inactivity gap in milliseconds.
     */
    int64_t inactivityGapMs() const { return inactivity_gap_ms_; }

    /**
     * Get the grace period in milliseconds.
     */
    int64_t graceMs() const { return grace_ms_; }

    /**
     * Create a new session window for a single timestamp.
     *
     * The initial session extends from (timestamp - gap) to (timestamp + gap),
     * allowing merging with adjacent sessions.
     *
     * @param timestamp_ms Event timestamp
     * @return Session window for this event
     */
    Window windowForTimestamp(int64_t timestamp_ms) const {
        // Initial session is just a point, but we set end = start + 1
        // to represent it as a window. During merging, gaps determine merging.
        return Window(timestamp_ms, timestamp_ms + 1);
    }

    /**
     * Check if two windows should be merged based on inactivity gap.
     *
     * @param window1 First window
     * @param window2 Second window
     * @return true if windows should be merged
     */
    bool shouldMerge(const Window& window1, const Window& window2) const {
        int64_t gap = 0;
        if (window1.endTime() <= window2.startTime()) {
            gap = window2.startTime() - window1.endTime();
        } else if (window2.endTime() <= window1.startTime()) {
            gap = window1.startTime() - window2.endTime();
        }
        return gap <= inactivity_gap_ms_;
    }

    /**
     * Merge two overlapping or adjacent sessions.
     *
     * @param window1 First window
     * @param window2 Second window
     * @return Merged window
     */
    Window merge(const Window& window1, const Window& window2) const {
        return Window(
            std::min(window1.startTime(), window2.startTime()),
            std::max(window1.endTime(), window2.endTime())
        );
    }

private:
    explicit SessionWindows(int64_t inactivity_gap_ms)
        : inactivity_gap_ms_(inactivity_gap_ms), grace_ms_(0) {}

    int64_t inactivity_gap_ms_;
    int64_t grace_ms_;
};

/**
 * SlidingWindows defines continuously advancing windows.
 *
 * Unlike hopping windows that advance by fixed steps, sliding windows
 * advance with each record, creating a new window for each unique
 * combination of records.
 *
 * This is primarily used for joins.
 */
class SlidingWindows {
public:
    /**
     * Create sliding windows with the given time difference.
     *
     * @param time_difference Maximum time difference between events in same window
     * @return SlidingWindows configuration
     */
    static SlidingWindows withTimeDifferenceAndGrace(Duration time_difference, Duration grace) {
        return SlidingWindows(time_difference.count(), grace.count());
    }

    /**
     * Get the time difference in milliseconds.
     */
    int64_t timeDifferenceMs() const { return time_difference_ms_; }

    /**
     * Get the grace period in milliseconds.
     */
    int64_t graceMs() const { return grace_ms_; }

private:
    SlidingWindows(int64_t time_difference_ms, int64_t grace_ms)
        : time_difference_ms_(time_difference_ms), grace_ms_(grace_ms) {}

    int64_t time_difference_ms_;
    int64_t grace_ms_;
};

/**
 * JoinWindows defines the window for stream-stream joins.
 *
 * A join window specifies how far in time (before and after) a record
 * on one side of the join can look for matching records on the other side.
 */
class JoinWindows {
public:
    /**
     * Create symmetric join windows.
     *
     * A record will be joined with records from the other stream that
     * arrive within (+/-) time_difference of this record.
     *
     * @param time_difference Maximum time difference for join
     * @return JoinWindows configuration
     */
    static JoinWindows of(Duration time_difference) {
        auto ms = time_difference.count();
        return JoinWindows(-ms, ms, 0);
    }

    /**
     * Set asymmetric time bounds for the join.
     *
     * @param before_ms How far back in time to look (positive value)
     * @param after_ms How far forward in time to look (positive value)
     * @return JoinWindows configuration
     */
    JoinWindows asymmetric(Duration before, Duration after) {
        before_ms_ = -before.count();
        after_ms_ = after.count();
        return *this;
    }

    /**
     * Set the grace period for late arrivals.
     *
     * @param grace Grace period duration
     * @return JoinWindows configuration
     */
    JoinWindows grace(Duration grace) {
        grace_ms_ = grace.count();
        return *this;
    }

    /**
     * Get the before time bound (negative value).
     */
    int64_t beforeMs() const { return before_ms_; }

    /**
     * Get the after time bound (positive value).
     */
    int64_t afterMs() const { return after_ms_; }

    /**
     * Get the grace period in milliseconds.
     */
    int64_t graceMs() const { return grace_ms_; }

    /**
     * Check if a timestamp from the other side matches this timestamp.
     *
     * @param this_timestamp Timestamp of record on this side
     * @param other_timestamp Timestamp of record on other side
     * @return true if the other record is within the join window
     */
    bool isWithinWindow(int64_t this_timestamp, int64_t other_timestamp) const {
        int64_t diff = other_timestamp - this_timestamp;
        return diff >= before_ms_ && diff <= after_ms_;
    }

private:
    JoinWindows(int64_t before_ms, int64_t after_ms, int64_t grace_ms)
        : before_ms_(before_ms), after_ms_(after_ms), grace_ms_(grace_ms) {}

    int64_t before_ms_;
    int64_t after_ms_;
    int64_t grace_ms_;
};

} // namespace streams
} // namespace kawasan
