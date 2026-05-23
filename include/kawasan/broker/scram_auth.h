#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace kawasan::broker {

// Phase 4.2b: SASL SCRAM-SHA-256 authentication (RFC 5802 + RFC 7677).
//
// SCRAM is a challenge-response mechanism that proves the client knows
// the password without sending it over the wire. The flow is:
//
//   client → server : "n,,n=<user>,r=<clientNonce>"          (client-first)
//   server → client : "r=<combined>,s=<salt>,i=<iter>"        (server-first)
//   client → server : "c=biws,r=<combined>,p=<clientProof>"   (client-final)
//   server → client : "v=<serverSignature>"                    (server-final)
//
// The server never stores the plaintext password. It stores:
//   StoredKey  = H(ClientKey)
//   ServerKey  = HMAC(SaltedPassword, "Server Key")
// where SaltedPassword = PBKDF2-HMAC-SHA-256(password, salt, iterations).
//
// `ScramAuthenticator` is a per-session state machine. It's instantiated
// once when the client picks the mechanism (via SaslHandshake), and
// `step()` is called for each SaslAuthenticate message. When done, it
// reports authenticated() = true (or an error code).
enum class ScramAlgorithm { kSha256, kSha512 };

class ScramCredentials {
public:
    // Phase 4.2b: helpers to construct credentials from a plaintext
    // password (used at config-load time). Production deployments should
    // store stored_key + server_key in a credentials file, not the
    // plaintext password.
    static ScramCredentials fromPassword(const std::string& password,
                                         int iterations = 4096,
                                         ScramAlgorithm algo = ScramAlgorithm::kSha256);

    ScramAlgorithm algorithm = ScramAlgorithm::kSha256;
    std::vector<uint8_t> salt;
    int iterations = 4096;
    std::vector<uint8_t> stored_key;  // H(ClientKey)
    std::vector<uint8_t> server_key;  // HMAC(SaltedPassword, "Server Key")
};

class ScramAuthenticator {
public:
    enum class Step { kClientFirst, kClientFinal, kDone };

    ScramAuthenticator(
        std::unordered_map<std::string, ScramCredentials> credentials,
        ScramAlgorithm algo);
    explicit ScramAuthenticator(
        std::unordered_map<std::string, ScramCredentials> credentials)
        : ScramAuthenticator(std::move(credentials), ScramAlgorithm::kSha256) {}

    /// @brief Process one SaslAuthenticate message.
    /// @param input The raw bytes from the client.
    /// @return The response bytes to send back. Empty vector on
    ///         authentication failure (caller should set error code).
    std::vector<uint8_t> step(const std::vector<uint8_t>& input);

    bool authenticated() const { return step_ == Step::kDone && success_; }
    bool failed() const { return failed_; }
    const std::string& username() const { return username_; }

private:
    std::unordered_map<std::string, ScramCredentials> credentials_;
    ScramAlgorithm algorithm_;
    Step step_ = Step::kClientFirst;
    bool success_ = false;
    bool failed_ = false;
    std::string username_;
    std::string client_nonce_;
    std::string server_nonce_;
    std::string client_first_bare_;  // "n=user,r=clientNonce"
    std::string server_first_message_;
    ScramCredentials user_creds_;
};

}  // namespace kawasan::broker
