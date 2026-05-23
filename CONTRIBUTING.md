# Contributing to Kawasan

Thank you for your interest in contributing to Kawasan! This document provides guidelines and instructions for contributing.

## Code of Conduct

Be respectful, inclusive, and professional in all interactions.

## Development Setup

### Prerequisites

- C++20 compatible compiler (GCC 11+, Clang 13+, or MSVC 19.29+)
- CMake 3.20+
- vcpkg or manual dependency installation
- Git

### Setting up the development environment

```bash
# Clone the repository
git clone https://github.com/kawasan/kawasan.git
cd kawasan

# Install dependencies using vcpkg
cmake -B build -DCMAKE_TOOLCHAIN_FILE=<vcpkg-root>/scripts/buildsystems/vcpkg.cmake

# Build
cmake --build build -j$(nproc)

# Run tests
cd build && ctest --output-on-failure
```

## Coding Standards

### C++ Style

- Follow C++20 best practices
- Use `.clang-format` for formatting (Google style with modifications)
- Use `.clang-tidy` for static analysis
- Format code before committing: `clang-format -i <file>`

### Naming Conventions

- **Namespaces**: `lower_case` (e.g., `kawasan::storage`)
- **Classes/Structs**: `CamelCase` (e.g., `LogSegment`, `Producer`)
- **Functions**: `camelBack` (e.g., `send()`, `getMetadata()`)
- **Variables**: `lower_case` (e.g., `offset`, `partition_id`)
- **Constants**: `UPPER_CASE` (e.g., `MAX_BATCH_SIZE`)
- **Member variables**: `lower_case_` with trailing underscore (e.g., `offset_`, `data_`)

### Code Quality

- Write unit tests for all new functionality
- Maintain test coverage above 80%
- Use RAII for resource management
- Prefer smart pointers over raw pointers
- Use `const` and `constexpr` where appropriate
- Document public APIs with Doxygen comments
- No compiler warnings (-Werror is enabled)

### Example

```cpp
namespace kawasan::storage {

/// @brief Manages a single log segment
class LogSegment {
public:
    /// @brief Creates a new log segment
    /// @param base_offset The starting offset for this segment
    /// @param path Directory path for segment files
    explicit LogSegment(int64_t base_offset, const std::string& path);
    
    /// @brief Appends records to the segment
    /// @param records The records to append
    /// @return The offset of the first appended record
    int64_t append(const std::vector<Record>& records);
    
private:
    int64_t base_offset_;
    std::unique_ptr<RocksDBStore> store_;
};

}  // namespace kawasan::storage
```

## Pull Request Process

1. **Fork** the repository
2. **Create a branch** with a descriptive name:
   - Feature: `feature/your-feature-name`
   - Bugfix: `fix/issue-description`
   - Docs: `docs/what-you-changed`

3. **Make your changes**:
   - Write clean, well-documented code
   - Add tests for new functionality
   - Update documentation if needed

4. **Test your changes**:
   ```bash
   cmake --build build
   cd build && ctest
   ```

5. **Format your code**:
   ```bash
   find src include -name "*.cpp" -o -name "*.h" | xargs clang-format -i
   ```

6. **Commit your changes**:
   - Use clear, descriptive commit messages
   - Reference issue numbers if applicable
   - Example: `feat: Add KRaft leader election (#123)`

7. **Push and create a pull request**:
   - Provide a clear description of changes
   - Link related issues
   - Ensure CI passes

### Commit Message Format

```
<type>: <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `perf`: Performance improvements
- `chore`: Maintenance tasks

## Testing

### Unit Tests

Place unit tests in `tests/unit/` with the suffix `_test.cpp`:

```cpp
#include <gtest/gtest.h>
#include <kawasan/storage/log_segment.h>

namespace kawasan::storage {

TEST(LogSegmentTest, AppendAndRead) {
    LogSegment segment(0, "/tmp/test-segment");
    
    std::vector<Record> records = {{"key1", "value1"}, {"key2", "value2"}};
    int64_t offset = segment.append(records);
    
    EXPECT_EQ(offset, 0);
    EXPECT_EQ(segment.size(), 2);
}

}  // namespace kawasan::storage
```

### Integration Tests

Place integration tests in `tests/integration/`:

```cpp
// Test full broker startup, producer/consumer interaction, etc.
```

## Documentation

- Update relevant documentation in `docs/`
- Add inline comments for complex logic
- Use Doxygen format for public APIs
- Update README.md if adding new features

## Areas for Contribution

### Good First Issues

- Documentation improvements
- Unit test additions
- Code cleanup and refactoring
- Bug fixes

### Advanced Topics

- Protocol implementation
- KRaft consensus
- Performance optimization
- Security features

## Questions?

- Open an issue for discussion
- Tag maintainers for guidance
- Check existing issues and PRs

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

