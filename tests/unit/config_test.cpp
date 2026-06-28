// Config accessor coercion (Phase A — final verification). Numeric/bool values
// supplied via env-var substitution or key=value property files arrive as JSON
// strings; get<int>/get<bool>/etc. must coerce them rather than throw, so
// env-substituted JSON configs (e.g. broker.production.properties) load.
#include <gtest/gtest.h>

#include "kawasan/common/config.h"

using kawasan::Config;

TEST(ConfigTest, CoercesNumericStringToInt) {
    Config c;
    c.setString("broker.id", "7");
    EXPECT_EQ(7, c.get<int>("broker.id", 0));
    EXPECT_EQ(7, c.get<int16_t>("broker.id", 0));
    EXPECT_EQ(7, c.get<int64_t>("broker.id", 0));
}

TEST(ConfigTest, CoercesNumericStringToLong) {
    Config c;
    c.setString("log.segment.bytes", "1073741824");
    EXPECT_EQ(1073741824LL, c.get<int64_t>("log.segment.bytes", 0));
}

TEST(ConfigTest, CoercesBoolStrings) {
    Config c;
    c.setString("authorizer.enabled", "true");
    c.setString("ssl.enabled", "false");
    EXPECT_TRUE(c.get<bool>("authorizer.enabled", false));
    EXPECT_FALSE(c.get<bool>("ssl.enabled", true));
}

TEST(ConfigTest, NativeTypesStillWork) {
    Config c;
    c.setInt("port", 9092);
    c.setBool("flag", true);
    EXPECT_EQ(9092, c.get<int>("port", 0));
    EXPECT_TRUE(c.get<bool>("flag", false));
}

TEST(ConfigTest, DefaultUsedWhenMissing) {
    Config c;
    EXPECT_EQ(42, c.get<int>("absent", 42));
    EXPECT_EQ("x", c.get<std::string>("absent", std::string("x")));
}

TEST(ConfigTest, StringValuePassesThroughUncoerced) {
    Config c;
    c.setString("host", "0.0.0.0");
    EXPECT_EQ("0.0.0.0", c.get<std::string>("host", std::string("")));
}

TEST(ConfigTest, InvalidNumericStringThrows) {
    Config c;
    c.setString("port", "not-a-number");
    EXPECT_THROW(c.get<int>("port", 0), std::exception);
}
