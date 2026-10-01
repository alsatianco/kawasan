#pragma once

#include <string>

namespace kawasan {

/// @brief Crash-atomically replaces `path` with `data`: writes `path.tmp`,
/// fsyncs it, then renames it over `path`. After a crash the file holds either
/// the old or the new contents, never a torn mix. Returns false (and removes
/// the temp file) on any failure; `path` is then unchanged.
bool writeFileAtomically(const std::string& path, const std::string& data);

}  // namespace kawasan
