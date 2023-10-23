#include "utils.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/time.h>
#include <utility>

namespace duckdb {

namespace {

constexpr char PATH_SEPARATOR = '/';

} // namespace

typename UtcClock::time_point UtcClock::now() noexcept { // NOLINT
	::timeval tm;
	if (::gettimeofday(&tm, nullptr) != 0) {
		std::cerr << "::gettimeofday failed" << std::endl;
		std::abort();
	}

	std::uint64_t count = static_cast<std::uint64_t>(tm.tv_sec) + static_cast<std::uint64_t>(tm.tv_usec) * 1'000'000;
	duration dur {count};
	return time_point {dur};
}

Path::Path(const char *raw) noexcept : components {} {
	std::string current_component;
	for (auto raw_ptr = raw; *raw_ptr; ++raw_ptr) {
		auto ch = *raw_ptr;
		if (ch == PATH_SEPARATOR) {
			if (!current_component.empty()) {
				components.push_back(std::move(current_component));
				current_component.clear();
			}
		} else {
			current_component.push_back(ch);
		}
	}

	if (!current_component.empty()) {
		components.push_back(std::move(current_component));
	}
}

Path::Path(const std::string &raw) noexcept : Path {raw.c_str()} {
}

std::string Path::GetFileName() const noexcept {
	if (components.empty()) {
		return "";
	} else {
		return components.back();
	}
}

Path Path::GetBase() const noexcept {
	if (IsRoot()) {
		return Path {};
	}

	Path ret;
	ret.components.resize(components.size() - 1);
	std::copy(components.begin(), components.end() - 1, ret.components.begin());

	return ret;
}

void Path::Push(const std::string &component) noexcept {
	components.push_back(component);
}

void Path::Push(const char *component) noexcept {
	components.emplace_back(component);
}

void Path::Pop() noexcept {
	components.pop_back();
}

std::string Path::ToString() const noexcept {
	std::ostringstream builder;
	for (const auto &c : components) {
		builder << PATH_SEPARATOR << c;
	}
	return builder.str();
}

Path::Path() noexcept : components {} {
}

bool operator==(const Path &lhs, const Path &rhs) noexcept {
	return lhs.components == rhs.components;
}

bool operator!=(const Path &lhs, const Path &rhs) noexcept {
	return lhs.components != rhs.components;
}

Path operator/(const Path &lhs, const char *component) noexcept {
	Path ret = lhs;
	ret.Push(component);
	return ret;
}

Path operator/(const Path &lhs, const std::string &component) noexcept {
	Path ret = lhs;
	ret.Push(component);
	return ret;
}

std::string GetEnv(const std::string &env) noexcept {
	auto ptr = std::getenv(env.c_str());
	std::string ret;
	if (ptr) {
		ret = std::string(ptr);
	}
	return ret;
}

std::string_view GetJdfsUsername() noexcept {
	static std::string JDFS_USERNAME([] {
		std::string username;
		auto ceph_args = GetEnv("CEPH_ARGS");
		if (!ceph_args.empty()) {
			auto pos = ceph_args.find("client");
			if (pos == std::string::npos) {
				return username;
			}
			auto space = ceph_args.find(' ', pos);
			username = ceph_args.substr(pos, space - pos);
		} else {
			username = GetEnv("SYS_JDFS_USERNAME");
			if (username.empty()) {
				username = GetEnv("JDFS_USERNAME");
			}
		}
		return username;
	}());
	return JDFS_USERNAME;
}

void ParseUrl(std::string_view url, std::string &pool_out, std::string &ns_out, std::string &path_out) {
	if (url.rfind("ceph://", 0) != 0) {
		throw std::runtime_error("URL needs to start ceph://");
	}
	auto pool_slash = url.find("//", 7);
	if (pool_slash == std::string::npos) {
		throw std::runtime_error("URL needs to contain a pool");
	}
	pool_out = url.substr(7, pool_slash - 7);
	if (pool_out.empty()) {
		throw std::runtime_error("URL needs to contain a non-empty pool");
	}
	auto ns_slash = url.find("//", pool_slash + 2);

	if (ns_slash == std::string::npos) {
		throw std::runtime_error("URL needs to contain a ns");
	}
	ns_out = url.substr(pool_slash + 2, ns_slash - pool_slash - 2);

	path_out = url.substr(ns_slash + 2);
}

} // namespace duckdb
