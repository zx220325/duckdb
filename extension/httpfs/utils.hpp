#pragma once

#include <chrono>
#include <cstdint>
#include <ratio>
#include <string>
#include <vector>

namespace duckdb {

struct UtcClock {
    using rep = std::uint64_t;
    using ratio = std::micro;
    using duration = std::chrono::duration<rep, ratio>;
    using time_point = std::chrono::time_point<UtcClock>;

    constexpr static bool is_steady = true;  // NOLINT

    static time_point now() noexcept;  // NOLINT
};

class Path {
public:
    explicit Path(const char *raw) noexcept;
    explicit Path(const std::string &raw) noexcept;

    bool IsEmpty() const noexcept { return components.empty(); }

    std::string GetFileName() const noexcept;

    Path GetBase() const noexcept;

    void Push(const std::string &component) noexcept;
    void Push(const char *component) noexcept;

    void Pop() noexcept;

    std::string ToString() const noexcept;

    const std::vector<std::string> &GetComponents() const noexcept { return components; }

    friend bool operator==(const Path &lhs, const Path &rhs) noexcept;
    friend bool operator!=(const Path &lhs, const Path &rhs) noexcept;

    friend Path operator/(const Path &lhs, const char *component) noexcept;
    friend Path operator/(const Path &lhs, const std::string &component) noexcept;

private:
    Path() noexcept;

    std::vector<std::string> components;
};

void ParseUrl(std::string_view url, std::string &pool_out, std::string &ns_out, std::string &path_out);

} // namespace duckdb
