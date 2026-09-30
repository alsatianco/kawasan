#pragma once

#include <rocksdb/db.h>

#include <memory>
#include <string>
#include <utility>

namespace kawasan {
namespace detail {

// RocksDB 11 removed the raw-pointer DB::Open overload in favor of
// std::unique_ptr<DB>*; older releases (e.g. the vcpkg baseline) only have the
// raw-pointer form. Dispatch on whichever exists.
template <typename DB>
rocksdb::Status openRocksDbImpl(const rocksdb::Options& options, const std::string& path,
                                std::unique_ptr<DB>& out) {
    if constexpr (requires(std::unique_ptr<DB>* p) { DB::Open(options, path, p); }) {
        return DB::Open(options, path, &out);
    } else {
        DB* raw = nullptr;
        rocksdb::Status status = DB::Open(options, path, &raw);
        out.reset(raw);
        return status;
    }
}

}  // namespace detail

inline rocksdb::Status openRocksDb(const rocksdb::Options& options, const std::string& path,
                                   std::unique_ptr<rocksdb::DB>& out) {
    return detail::openRocksDbImpl<rocksdb::DB>(options, path, out);
}

}  // namespace kawasan
