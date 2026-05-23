#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace kawasan::broker {

// Phase 4.2c: in-memory ACL binding store.
//
// Bindings are deduplicated by the tuple (resource_type, resource_name,
// pattern_type, principal, host, operation, permission_type). A
// `match(filter, binding)` predicate respects Kafka's ANY/MATCH/LITERAL
// pattern semantics so that DescribeAcls and DeleteAcls can be honored
// with the standard CLI / Java AdminClient filter shape.
//
// Note: this store *only* records bindings. Enforcement (i.e. an actual
// authorizer that consults the store on each request) is a follow-up
// — same as Kafka with its `allow.everyone.if.no.acl.found=true`
// configuration semantics.
class AclStore {
public:
    struct Binding {
        int8_t resource_type;
        std::string resource_name;
        int8_t pattern_type;
        std::string principal;
        std::string host;
        int8_t operation;
        int8_t permission_type;

        bool operator==(const Binding& o) const {
            return resource_type == o.resource_type &&
                   resource_name == o.resource_name &&
                   pattern_type == o.pattern_type && principal == o.principal &&
                   host == o.host && operation == o.operation &&
                   permission_type == o.permission_type;
        }
    };

    struct Filter {
        int8_t resource_type = 1;          // ANY
        std::string resource_name_filter;  // empty = ANY
        int8_t pattern_type = 1;           // ANY
        std::string principal_filter;
        std::string host_filter;
        int8_t operation = 1;              // ANY
        int8_t permission_type = 1;        // ANY
    };

    /// @brief Adds a binding. Idempotent.
    void add(const Binding& b);

    /// @brief Returns all bindings that match the filter.
    std::vector<Binding> describe(const Filter& f) const;

    /// @brief Removes all bindings matching the filter. Returns the
    /// removed bindings so callers can echo them in DeleteAclsResponse.
    std::vector<Binding> remove(const Filter& f);

    /// @brief Returns the total binding count (testing aid).
    size_t size() const;

private:
    static bool matches(const Filter& f, const Binding& b);

    mutable std::mutex mutex_;
    std::vector<Binding> bindings_;
};

}  // namespace kawasan::broker
