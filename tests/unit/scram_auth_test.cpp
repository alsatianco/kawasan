#include <gtest/gtest.h>

#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include <cstring>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "kawasan/broker/scram_auth.h"

using kawasan::broker::ScramAlgorithm;
using kawasan::broker::ScramAuthenticator;
using kawasan::broker::ScramCredentials;

namespace {

// Minimal client-side SCRAM helper. The point of these tests is that a
// correctly-implemented client (RFC 5802) can authenticate against our
// ScramAuthenticator.
struct ClientHelper {
    ScramAlgorithm algo;
    size_t hash_len;
    const EVP_MD* md;

    explicit ClientHelper(ScramAlgorithm a)
        : algo(a),
          hash_len(a == ScramAlgorithm::kSha512 ? 64 : 32),
          md(a == ScramAlgorithm::kSha512 ? EVP_sha512() : EVP_sha256()) {}

    std::vector<uint8_t> hmac(const std::vector<uint8_t>& key,
                              const std::string& data) const {
        std::vector<uint8_t> out(hash_len);
        unsigned int outlen = 0;
        HMAC(md, key.data(), static_cast<int>(key.size()),
             reinterpret_cast<const unsigned char*>(data.data()), data.size(),
             out.data(), &outlen);
        return out;
    }

    std::vector<uint8_t> hash(const std::vector<uint8_t>& data) const {
        std::vector<uint8_t> out(hash_len);
        unsigned int outlen = 0;
        EVP_Digest(data.data(), data.size(), out.data(), &outlen, md, nullptr);
        return out;
    }

    std::vector<uint8_t> pbkdf2(const std::string& password,
                                const std::vector<uint8_t>& salt,
                                int iterations) const {
        std::vector<uint8_t> out(hash_len);
        PKCS5_PBKDF2_HMAC(password.data(), static_cast<int>(password.size()),
                          salt.data(), static_cast<int>(salt.size()),
                          iterations, md,
                          static_cast<int>(out.size()), out.data());
        return out;
    }

    std::string b64(const std::vector<uint8_t>& data) const {
        static constexpr char kTable[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        std::string out;
        out.reserve((data.size() + 2) / 3 * 4);
        size_t i = 0;
        for (; i + 2 < data.size(); i += 3) {
            const uint32_t n = (data[i] << 16) | (data[i + 1] << 8) | data[i + 2];
            out.push_back(kTable[(n >> 18) & 0x3F]);
            out.push_back(kTable[(n >> 12) & 0x3F]);
            out.push_back(kTable[(n >> 6) & 0x3F]);
            out.push_back(kTable[n & 0x3F]);
        }
        if (i < data.size()) {
            uint32_t n = data[i] << 16;
            if (i + 1 < data.size()) n |= data[i + 1] << 8;
            out.push_back(kTable[(n >> 18) & 0x3F]);
            out.push_back(kTable[(n >> 12) & 0x3F]);
            out.push_back(i + 1 < data.size() ? kTable[(n >> 6) & 0x3F] : '=');
            out.push_back('=');
        }
        return out;
    }

    std::vector<uint8_t> b64decode(const std::string& s) const {
        int8_t inv[256];
        std::memset(inv, -1, sizeof(inv));
        const char* tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for (int i = 0; i < 64; ++i) inv[static_cast<uint8_t>(tbl[i])] = i;
        std::vector<uint8_t> out;
        uint32_t buf = 0;
        int bits = 0;
        for (char c : s) {
            if (c == '=') break;
            int v = inv[static_cast<uint8_t>(c)];
            if (v < 0) continue;
            buf = (buf << 6) | v;
            bits += 6;
            if (bits >= 8) {
                bits -= 8;
                out.push_back(static_cast<uint8_t>((buf >> bits) & 0xFF));
            }
        }
        return out;
    }
};

std::string scramField(const std::string& s, char key) {
    size_t i = 0;
    while (i < s.size()) {
        if (i + 1 < s.size() && s[i] == key && s[i + 1] == '=') {
            const size_t start = i + 2;
            const size_t end = s.find(',', start);
            return s.substr(start, end - start);
        }
        const size_t next = s.find(',', i);
        if (next == std::string::npos) break;
        i = next + 1;
    }
    return {};
}

void runScramFlow(ScramAlgorithm algo, const std::string& user,
                  const std::string& password, bool expect_success) {
    std::unordered_map<std::string, ScramCredentials> creds;
    creds.emplace(user, ScramCredentials::fromPassword(password, 4096, algo));

    ScramAuthenticator auth(creds, algo);
    ClientHelper c(algo);

    // Step 1: client-first
    const std::string client_nonce = "fyko+d2lbbFgONRv9qkxdawL";  // deterministic
    const std::string client_first_bare = "n=" + user + ",r=" + client_nonce;
    const std::string client_first = "n,," + client_first_bare;

    auto server_first = auth.step({client_first.begin(), client_first.end()});
    if (!expect_success) {
        EXPECT_TRUE(auth.failed());
        return;
    }
    ASSERT_FALSE(auth.failed()) << "server-first failed";
    const std::string server_first_str(server_first.begin(), server_first.end());

    const std::string combined_nonce = scramField(server_first_str, 'r');
    const std::string salt_b64 = scramField(server_first_str, 's');
    const std::string iter_str = scramField(server_first_str, 'i');
    ASSERT_FALSE(combined_nonce.empty());
    ASSERT_FALSE(salt_b64.empty());

    const auto salt = c.b64decode(salt_b64);
    const int iterations = std::stoi(iter_str);

    // Compute proof
    const auto salted_password = c.pbkdf2(password, salt, iterations);
    const auto client_key = c.hmac(salted_password, "Client Key");
    const auto stored_key = c.hash(client_key);

    const std::string client_final_no_proof =
        "c=biws,r=" + combined_nonce;
    const std::string auth_message =
        client_first_bare + "," + server_first_str + "," + client_final_no_proof;
    const auto client_signature = c.hmac(stored_key, auth_message);
    std::vector<uint8_t> client_proof(c.hash_len);
    for (size_t i = 0; i < c.hash_len; ++i) {
        client_proof[i] = client_key[i] ^ client_signature[i];
    }
    const std::string client_final =
        client_final_no_proof + ",p=" + c.b64(client_proof);

    auto server_final = auth.step({client_final.begin(), client_final.end()});
    ASSERT_TRUE(auth.authenticated()) << "expected auth success";
    EXPECT_FALSE(server_final.empty());
    EXPECT_EQ(auth.username(), user);
}

TEST(ScramAuthTest, Sha256RoundTrip) {
    runScramFlow(ScramAlgorithm::kSha256, "alice", "wonderland42", true);
}

TEST(ScramAuthTest, Sha512RoundTrip) {
    runScramFlow(ScramAlgorithm::kSha512, "bob", "secret-pass!", true);
}

TEST(ScramAuthTest, RejectsUnknownUser) {
    // Helper expects auth failure on unknown user.
    std::unordered_map<std::string, ScramCredentials> creds;
    creds.emplace("known", ScramCredentials::fromPassword("pw", 4096));
    ScramAuthenticator auth(creds, ScramAlgorithm::kSha256);
    const std::string m = "n,,n=unknown,r=ABCDEF";
    auto out = auth.step({m.begin(), m.end()});
    EXPECT_TRUE(auth.failed());
    EXPECT_TRUE(out.empty());
}

}  // namespace
