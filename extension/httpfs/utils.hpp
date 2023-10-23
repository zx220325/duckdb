#pragma once

#include <chrono>
#include <cstdint>
#include <ratio>
#include <string>
#include <string_view>

namespace duckdb {

struct UtcClock {
    using rep = std::uint64_t;
    using ratio = std::micro;
    using duration = std::chrono::duration<rep, ratio>;
    using time_point = std::chrono::time_point<UtcClock>;

    constexpr static bool is_steady = true;  // NOLINT

    static time_point now() noexcept;  // NOLINT
};

std::string GetEnv(const std::string &env) noexcept;

std::string_view GetJdfsUsername() noexcept;

void ParseUrl(std::string_view url, std::string &pool_out, std::string &ns_out, std::string &path_out);

} // namespace duckdb
