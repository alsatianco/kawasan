#include <gtest/gtest.h>

#include "kawasan/broker/acl_store.h"

using kawasan::broker::AclStore;

namespace {

AclStore::Binding makeBinding(const std::string& name, int8_t op = 3,
                              int8_t perm = 3, int8_t rt = 2, int8_t pt = 3) {
    AclStore::Binding b;
    b.resource_type = rt;
    b.resource_name = name;
    b.pattern_type = pt;
    b.principal = "User:test";
    b.host = "*";
    b.operation = op;
    b.permission_type = perm;
    return b;
}

TEST(AclStoreTest, AddIsIdempotent) {
    AclStore s;
    s.add(makeBinding("topic-a"));
    s.add(makeBinding("topic-a"));
    EXPECT_EQ(s.size(), 1u);
}

TEST(AclStoreTest, DescribeByExactName) {
    AclStore s;
    s.add(makeBinding("topic-a"));
    s.add(makeBinding("topic-b"));

    AclStore::Filter f;
    f.resource_type = 2;  // TOPIC
    f.resource_name_filter = "topic-a";
    f.pattern_type = 1;   // ANY
    auto results = s.describe(f);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].resource_name, "topic-a");
}

TEST(AclStoreTest, DescribeAnyMatchesAll) {
    AclStore s;
    s.add(makeBinding("topic-a"));
    s.add(makeBinding("topic-b"));

    AclStore::Filter f;  // ANY/ANY/ANY default
    auto results = s.describe(f);
    EXPECT_EQ(results.size(), 2u);
}

TEST(AclStoreTest, RemoveReturnsRemovedBindings) {
    AclStore s;
    s.add(makeBinding("topic-a"));
    s.add(makeBinding("topic-b"));

    AclStore::Filter f;
    f.resource_name_filter = "topic-a";
    auto removed = s.remove(f);
    EXPECT_EQ(removed.size(), 1u);
    EXPECT_EQ(removed[0].resource_name, "topic-a");
    EXPECT_EQ(s.size(), 1u);
}

TEST(AclStoreTest, MatchPatternFindsPrefixed) {
    AclStore s;
    AclStore::Binding b = makeBinding("prefix-", 3, 3, 2, 4);  // PREFIXED
    s.add(b);

    AclStore::Filter f;
    f.pattern_type = 2;  // MATCH
    f.resource_name_filter = "prefix-foo";
    auto results = s.describe(f);
    EXPECT_EQ(results.size(), 1u);
}

TEST(AclStoreTest, FilterByOperation) {
    AclStore s;
    s.add(makeBinding("t", /*op=*/3));  // READ
    s.add(makeBinding("t", /*op=*/4));  // WRITE

    AclStore::Filter f;
    f.operation = 3;  // READ
    auto results = s.describe(f);
    EXPECT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].operation, 3);
}

}  // namespace
