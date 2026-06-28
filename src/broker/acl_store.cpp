#include "kawasan/broker/acl_store.h"

#include <algorithm>

namespace kawasan::broker {

namespace {

constexpr int8_t kAnyResourceType = 1;
constexpr int8_t kAnyPatternType = 1;
constexpr int8_t kMatchPatternType = 2;
constexpr int8_t kLiteralPatternType = 3;
constexpr int8_t kPrefixedPatternType = 4;
constexpr int8_t kAnyOperation = 1;
constexpr int8_t kAnyPermission = 1;

// String filter semantics: empty string in the filter means "ANY". Kafka
// also uses NULL but our type system already coalesces null → "" before
// reaching here.
bool stringMatches(const std::string& filter, const std::string& value) {
    if (filter.empty()) return true;
    return filter == value;
}

bool patternMatches(int8_t filter_pt, int8_t binding_pt,
                    const std::string& filter_name,
                    const std::string& binding_name) {
    if (filter_pt == kAnyPatternType) {
        // ANY pattern type: name filter is treated as exact-or-empty.
        return stringMatches(filter_name, binding_name);
    }
    if (filter_pt == kMatchPatternType) {
        // MATCH: filter matches LITERAL exact, or PREFIXED if the binding's
        // name is a prefix of the filter name. This is a simplification of
        // Kafka's full MATCH semantics; sufficient for the common CLI uses.
        if (filter_name.empty()) return true;
        if (binding_pt == kLiteralPatternType) return filter_name == binding_name;
        if (binding_pt == kPrefixedPatternType) {
            return filter_name.compare(0, binding_name.size(), binding_name) == 0;
        }
        return false;
    }
    // LITERAL / PREFIXED: pattern_type must match exactly, names must match.
    if (filter_pt != binding_pt) return false;
    return stringMatches(filter_name, binding_name);
}

}  // namespace

void AclStore::add(const Binding& b) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (std::find(bindings_.begin(), bindings_.end(), b) == bindings_.end()) {
        bindings_.push_back(b);
    }
}

bool AclStore::matches(const Filter& f, const Binding& b) {
    if (f.resource_type != kAnyResourceType && f.resource_type != b.resource_type) return false;
    if (f.operation != kAnyOperation && f.operation != b.operation) return false;
    if (f.permission_type != kAnyPermission && f.permission_type != b.permission_type) return false;
    if (!stringMatches(f.principal_filter, b.principal)) return false;
    if (!stringMatches(f.host_filter, b.host)) return false;
    if (!patternMatches(f.pattern_type, b.pattern_type,
                        f.resource_name_filter, b.resource_name)) return false;
    return true;
}

std::vector<AclStore::Binding> AclStore::describe(const Filter& f) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Binding> out;
    for (const auto& b : bindings_) {
        if (matches(f, b)) out.push_back(b);
    }
    return out;
}

std::vector<AclStore::Binding> AclStore::remove(const Filter& f) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Binding> removed;
    auto it = bindings_.begin();
    while (it != bindings_.end()) {
        if (matches(f, *it)) {
            removed.push_back(*it);
            it = bindings_.erase(it);
        } else {
            ++it;
        }
    }
    return removed;
}

size_t AclStore::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return bindings_.size();
}

bool AclStore::authorize(const std::string& principal, int8_t operation,
                         int8_t resource_type, const std::string& resource_name,
                         const std::string& host, bool allow_if_no_acl) const {
    constexpr int8_t kAllOperation = 2;
    constexpr int8_t kDenyPermission = 2;
    constexpr int8_t kAllowPermission = 3;

    std::lock_guard<std::mutex> lock(mutex_);
    bool matched_allow = false;
    for (const auto& b : bindings_) {
        if (b.resource_type != resource_type) continue;
        if (b.operation != operation && b.operation != kAllOperation) continue;
        if (b.principal != principal && b.principal != "User:*") continue;
        if (!b.host.empty() && b.host != "*" && b.host != host) continue;

        bool name_ok = false;
        if (b.pattern_type == kLiteralPatternType) {
            name_ok = (b.resource_name == "*") || (b.resource_name == resource_name);
        } else if (b.pattern_type == kPrefixedPatternType) {
            name_ok = resource_name.size() >= b.resource_name.size() &&
                      resource_name.compare(0, b.resource_name.size(), b.resource_name) == 0;
        }
        if (!name_ok) continue;

        // A matching DENY is decisive (DENY beats ALLOW); keep scanning for a
        // DENY even after seeing an ALLOW.
        if (b.permission_type == kDenyPermission) return false;
        if (b.permission_type == kAllowPermission) matched_allow = true;
    }
    if (matched_allow) return true;
    return allow_if_no_acl;
}

}  // namespace kawasan::broker
