#include "kawasan/common/file_util.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>

namespace kawasan {

bool writeFileAtomically(const std::string& path, const std::string& data) {
    const std::string tmp = path + ".tmp";
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return false;
    }
    size_t written = 0;
    bool ok = true;
    while (written < data.size()) {
        const ssize_t n = ::write(fd, data.data() + written, data.size() - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            ok = false;
            break;
        }
        written += static_cast<size_t>(n);
    }
    ok = ok && ::fsync(fd) == 0;
    ok = (::close(fd) == 0) && ok;
    ok = ok && ::rename(tmp.c_str(), path.c_str()) == 0;
    if (!ok) {
        ::unlink(tmp.c_str());
    }
    return ok;
}

}  // namespace kawasan
