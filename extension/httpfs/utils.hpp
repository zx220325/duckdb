#pragma once

#include <string>
#include <string_view>

namespace duckdb {

std::string GetEnv(const std::string &env) noexcept;

std::string_view GetJdfsUsername() noexcept;

void ParseUrl(std::string_view url, std::string &pool_out, std::string &ns_out, std::string &path_out);

} // namespace duckdb
