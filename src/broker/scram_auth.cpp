#include "kawasan/broker/scram_auth.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include <cstring>
#include <random>
#include <sstream>
#include <stdexcept>

#include "kawasan/common/logger.h"

namespace kawasan::broker {

namespace {

// Phase 4.2b: dispatch on algorithm. SHA-256 → 32-byte output;
// SHA-512 → 64-byte output. Same code path otherwise.
const EVP_MD* evpFor(ScramAlgorithm algo) {
    return algo == ScramAlgorithm::kSha512 ? EVP_sha512() : EVP_sha256();
}

size_t hashLen(ScramAlgorithm algo) {
    return algo == ScramAlgorithm::kSha512 ? 64 : 32;
}

std::vector<uint8_t> hashFn(const uint8_t* data, size_t len,
                            ScramAlgorithm algo) {
    std::vector<uint8_t> out(hashLen(algo));
    unsigned int outlen = 0;
    EVP_Digest(data, len, out.data(), &outlen, evpFor(algo), nullptr);
    return out;
}

std::vector<uint8_t> hmacFn(const std::vector<uint8_t>& key,
                            const std::string& data,
                            ScramAlgorithm algo) {
    std::vector<uint8_t> out(hashLen(algo));
    unsigned int outlen = 0;
    HMAC(evpFor(algo), key.data(), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(),
         out.data(), &outlen);
    return out;
}

std::vector<uint8_t> pbkdf2Fn(const std::string& password,
                              const std::vector<uint8_t>& salt,
                              int iterations,
                              ScramAlgorithm algo) {
    std::vector<uint8_t> out(hashLen(algo));
    PKCS5_PBKDF2_HMAC(password.data(), static_cast<int>(password.size()),
                      salt.data(), static_cast<int>(salt.size()),
                      iterations, evpFor(algo),
                      static_cast<int>(out.size()), out.data());
    return out;
}

std::string base64_encode(const std::vector<uint8_t>& data) {
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

std::vector<uint8_t> base64_decode(const std::string& s) {
    static int8_t kInv[256];
    static bool inited = false;
    if (!inited) {
        std::memset(kInv, -1, sizeof(kInv));
        const char* tbl =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for (int i = 0; i < 64; ++i) kInv[static_cast<uint8_t>(tbl[i])] = i;
        inited = true;
    }
    std::vector<uint8_t> out;
    out.reserve(s.size() * 3 / 4);
    uint32_t buf = 0;
    int bits = 0;
    for (char c : s) {
        if (c == '=') break;
        int v = kInv[static_cast<uint8_t>(c)];
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

std::string randomNonce(size_t len = 24) {
    std::vector<uint8_t> bytes(len);
    RAND_bytes(bytes.data(), static_cast<int>(bytes.size()));
    // SCRAM nonces must be printable ASCII (no commas).
    static constexpr char kAlpha[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    std::string out(len, 'a');
    for (size_t i = 0; i < len; ++i) {
        out[i] = kAlpha[bytes[i] % (sizeof(kAlpha) - 1)];
    }
    return out;
}

// Parse a SCRAM key=value comma list. Returns the value for `key` or empty.
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

}  // namespace

ScramCredentials ScramCredentials::fromPassword(const std::string& password,
                                                int iterations,
                                                ScramAlgorithm algo) {
    ScramCredentials c;
    c.algorithm = algo;
    c.iterations = iterations;
    c.salt.resize(16);
    RAND_bytes(c.salt.data(), static_cast<int>(c.salt.size()));

    const auto salted_password = pbkdf2Fn(password, c.salt, iterations, algo);
    const auto client_key = hmacFn(salted_password, "Client Key", algo);
    c.stored_key = hashFn(client_key.data(), client_key.size(), algo);
    c.server_key = hmacFn(salted_password, "Server Key", algo);
    return c;
}

ScramAuthenticator::ScramAuthenticator(
    std::unordered_map<std::string, ScramCredentials> credentials,
    ScramAlgorithm algo)
    : credentials_(std::move(credentials)), algorithm_(algo) {}

std::vector<uint8_t> ScramAuthenticator::step(
    const std::vector<uint8_t>& input) {
    std::string msg(input.begin(), input.end());

    if (step_ == Step::kClientFirst) {
        // Expected: "n,,n=username,r=clientNonce" (gs2-header + client-first-bare).
        if (msg.size() < 3 || msg.substr(0, 3) != "n,,") {
            failed_ = true;
            return {};
        }
        client_first_bare_ = msg.substr(3);
        username_ = scramField(client_first_bare_, 'n');
        client_nonce_ = scramField(client_first_bare_, 'r');
        if (username_.empty() || client_nonce_.empty()) {
            failed_ = true;
            return {};
        }
        auto it = credentials_.find(username_);
        if (it == credentials_.end()) {
            // Per RFC 5802 §7, we still send a server-first message to
            // avoid a username-existence side channel, but we use a fake
            // credential and the client-final check will fail.
            // For simplicity here, fail immediately. A production
            // implementation should mock the credentials.
            failed_ = true;
            return {};
        }
        user_creds_ = it->second;
        server_nonce_ = randomNonce();
        const std::string combined_nonce = client_nonce_ + server_nonce_;
        server_first_message_ = "r=" + combined_nonce +
                                ",s=" + base64_encode(user_creds_.salt) +
                                ",i=" + std::to_string(user_creds_.iterations);
        step_ = Step::kClientFinal;
        return std::vector<uint8_t>(server_first_message_.begin(),
                                    server_first_message_.end());
    }

    if (step_ == Step::kClientFinal) {
        // Expected: "c=biws,r=<combinedNonce>,p=<clientProof>"
        const std::string channel_binding = scramField(msg, 'c');
        const std::string nonce = scramField(msg, 'r');
        const std::string proof_b64 = scramField(msg, 'p');
        if (channel_binding != "biws" /* base64("n,,") */) {
            failed_ = true;
            return {};
        }
        if (nonce != client_nonce_ + server_nonce_) {
            failed_ = true;
            return {};
        }
        // client-final-message-without-proof: drop ",p=..."
        const size_t pcomma = msg.rfind(",p=");
        if (pcomma == std::string::npos) {
            failed_ = true;
            return {};
        }
        const std::string client_final_no_proof = msg.substr(0, pcomma);

        const std::string auth_message = client_first_bare_ + "," +
                                         server_first_message_ + "," +
                                         client_final_no_proof;
        const auto client_signature =
            hmacFn(user_creds_.stored_key, auth_message, algorithm_);
        const auto client_proof = base64_decode(proof_b64);
        if (client_proof.size() != client_signature.size()) {
            failed_ = true;
            return {};
        }
        // ClientKey = ClientProof XOR ClientSignature
        const size_t hashLenBytes = hashLen(algorithm_);
        std::vector<uint8_t> client_key(hashLenBytes);
        for (size_t i = 0; i < hashLenBytes; ++i) {
            client_key[i] = client_proof[i] ^ client_signature[i];
        }
        const auto stored_key_calc =
            hashFn(client_key.data(), client_key.size(), algorithm_);
        if (stored_key_calc != user_creds_.stored_key) {
            failed_ = true;
            return {};
        }
        const auto server_signature =
            hmacFn(user_creds_.server_key, auth_message, algorithm_);
        const std::string final = "v=" + base64_encode(server_signature);
        success_ = true;
        step_ = Step::kDone;
        Logger::info("SCRAM-{}: user '{}' authenticated",
                     algorithm_ == ScramAlgorithm::kSha512 ? "SHA-512" : "SHA-256",
                     username_);
        return std::vector<uint8_t>(final.begin(), final.end());
    }

    failed_ = true;
    return {};
}

}  // namespace kawasan::broker
