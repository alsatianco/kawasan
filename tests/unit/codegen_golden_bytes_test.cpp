// Phase EX-3: byte-for-byte equivalence between hand-written and
// schema-generated encoders. This is the contract that lets us migrate
// individual APIs to generated code without changing wire behavior.
//
// We encode the same logical payload through both encoders and assert
// the resulting byte sequences are identical. Run this test in CI for
// any API we migrate to codegen.
#include <gtest/gtest.h>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/api_versions.h"
#include "kawasan/protocol/sasl_request.h"
#include "kawasan/protocol/generated/ApiVersionsRequest.h"
#include "kawasan/protocol/generated/ApiVersionsResponse.h"
#include "kawasan/protocol/generated/SaslHandshakeRequest.h"

using kawasan::Buffer;

namespace {

// Helper: convert a Buffer's contents to a copyable byte vector.
std::vector<uint8_t> snapshot(const Buffer& b) {
    return std::vector<uint8_t>(b.data(), b.data() + b.size());
}

TEST(CodegenGoldenBytes, ApiVersionsRequestV0EqualsHandWritten) {
    // v0 has no payload at all (empty request body).
    kawasan::protocol::ApiVersionsRequest hand;
    kawasan::protocol::generated::ApiVersionsRequest gen;

    Buffer hb, gb;
    hand.encode(hb, /*api_version=*/0);
    gen.encode(gb, /*api_version=*/0);

    EXPECT_EQ(snapshot(hb), snapshot(gb));
}

TEST(CodegenGoldenBytes, ApiVersionsRequestV3WithSoftwareName) {
    kawasan::protocol::ApiVersionsRequest hand;
    hand.setClientSoftwareName("kawasan-test");
    hand.setClientSoftwareVersion("1.0.0");

    kawasan::protocol::generated::ApiVersionsRequest gen;
    gen.client_software_name_ = "kawasan-test";
    gen.client_software_version_ = "1.0.0";

    Buffer hb, gb;
    hand.encode(hb, /*api_version=*/3);
    gen.encode(gb, /*api_version=*/3);

    EXPECT_EQ(snapshot(hb), snapshot(gb))
        << "v3 ApiVersionsRequest encoding diverged between hand-written and generated";
}

TEST(CodegenGoldenBytes, ApiVersionsRequestV4EqualsHandWritten) {
    kawasan::protocol::ApiVersionsRequest hand;
    hand.setClientSoftwareName("librdkafka");
    hand.setClientSoftwareVersion("2.5.0");

    kawasan::protocol::generated::ApiVersionsRequest gen;
    gen.client_software_name_ = "librdkafka";
    gen.client_software_version_ = "2.5.0";

    Buffer hb, gb;
    hand.encode(hb, /*api_version=*/4);
    gen.encode(gb, /*api_version=*/4);

    EXPECT_EQ(snapshot(hb), snapshot(gb));
}

TEST(CodegenGoldenBytes, ApiVersionsResponseV0EqualsHandWritten) {
    kawasan::protocol::ApiVersionsResponse hand;
    hand.setErrorCode(kawasan::ErrorCode::NONE);
    hand.addApiVersion({kawasan::protocol::ApiKey::PRODUCE, 0, 9});
    hand.addApiVersion({kawasan::protocol::ApiKey::FETCH, 0, 13});

    kawasan::protocol::generated::ApiVersionsResponse gen;
    gen.error_code_ = 0;
    gen.api_keys_.push_back({static_cast<int16_t>(kawasan::protocol::ApiKey::PRODUCE), 0, 9});
    gen.api_keys_.push_back({static_cast<int16_t>(kawasan::protocol::ApiKey::FETCH), 0, 13});

    Buffer hb, gb;
    hand.encode(hb, /*api_version=*/0);
    gen.encode(gb, /*api_version=*/0);

    EXPECT_EQ(snapshot(hb), snapshot(gb));
}

TEST(CodegenGoldenBytes, ApiVersionsResponseV3FlexibleEncoding) {
    kawasan::protocol::ApiVersionsResponse hand;
    hand.setErrorCode(kawasan::ErrorCode::NONE);
    hand.setThrottleTimeMs(0);
    hand.addApiVersion({kawasan::protocol::ApiKey::METADATA, 0, 13});

    kawasan::protocol::generated::ApiVersionsResponse gen;
    gen.error_code_ = 0;
    gen.throttle_time_ms_ = 0;
    gen.api_keys_.push_back({static_cast<int16_t>(kawasan::protocol::ApiKey::METADATA), 0, 13});

    Buffer hb, gb;
    hand.encode(hb, /*api_version=*/3);
    gen.encode(gb, /*api_version=*/3);

    EXPECT_EQ(snapshot(hb), snapshot(gb))
        << "v3 ApiVersionsResponse compact-array + tagged-fields encoding diverged";
}

// KIP-152: SaslHandshake stays non-flexible at every version. The
// generator handles this via flexibleVersions:"none" which collapses
// every flex-guard `if` to `if (false)` — the compiler DCEs it,
// producing the exact same wire bytes as the hand-written path. If
// the dead-code elimination contract ever breaks, this test fails.
TEST(CodegenGoldenBytes, SaslHandshakeRequestV0EqualsHandWritten) {
    kawasan::protocol::SaslHandshakeRequest hand;
    hand.setMechanism("PLAIN");

    kawasan::protocol::generated::SaslHandshakeRequest gen;
    gen.mechanism_ = "PLAIN";

    Buffer hb, gb;
    hand.encode(hb, /*api_version=*/0);
    gen.encode(gb, /*api_version=*/0);

    EXPECT_EQ(snapshot(hb), snapshot(gb));
}

TEST(CodegenGoldenBytes, SaslHandshakeRequestV1NeverFlexible) {
    // The bug we hit in the EX-1 era: SaslHandshake v1 was being
    // treated as flexible at the request-header layer, breaking real
    // Java clients. This test would have caught that at unit-test
    // time. Both encoders must emit a plain STRING (int16 length +
    // bytes) — NOT a COMPACT_STRING, NOT a tagged-fields trailer.
    kawasan::protocol::SaslHandshakeRequest hand;
    hand.setMechanism("SCRAM-SHA-256");

    kawasan::protocol::generated::SaslHandshakeRequest gen;
    gen.mechanism_ = "SCRAM-SHA-256";

    Buffer hb, gb;
    hand.encode(hb, /*api_version=*/1);
    gen.encode(gb, /*api_version=*/1);

    EXPECT_EQ(snapshot(hb), snapshot(gb))
        << "v1 SaslHandshake encoding diverged — KIP-152 non-flexible "
           "behavior must be preserved";
}

// Round-trip: encode with generated, decode with hand-written → identical fields.
TEST(CodegenGoldenBytes, ApiVersionsRequestRoundTripV3) {
    kawasan::protocol::generated::ApiVersionsRequest gen_in;
    gen_in.client_software_name_ = "interop-test";
    gen_in.client_software_version_ = "9.9.9";

    Buffer b;
    gen_in.encode(b, 3);

    std::vector<uint8_t> bytes(b.data(), b.data() + b.size());
    Buffer rb(std::move(bytes));
    kawasan::protocol::ApiVersionsRequest hand_out;
    hand_out.decode(rb, 3);

    EXPECT_EQ(hand_out.clientSoftwareName(), "interop-test");
    EXPECT_EQ(hand_out.clientSoftwareVersion(), "9.9.9");
}

}  // namespace
