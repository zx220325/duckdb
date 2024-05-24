#include "utils.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/time.h>
#include <tuple>
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

	std::uint64_t usec_count =
	    static_cast<std::uint64_t>(tm.tv_sec) * 1'000'000 + static_cast<std::uint64_t>(tm.tv_usec);
	std::chrono::microseconds dur {usec_count};
	return time_point {dur};
}

Path::Path(const char *raw) noexcept : components {} {
	std::string current_component;
	for (auto raw_ptr = raw; *raw_ptr; ++raw_ptr) {
		auto ch = *raw_ptr;
		current_component.push_back(ch);

		if (ch == PATH_SEPARATOR) {
			components.push_back(std::move(current_component));
			current_component.clear();
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
	if (IsEmpty()) {
		return Path {};
	}

	Path ret = *this;
	ret.components.pop_back();

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
		builder << c;
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

std::string GetEnv(const std::string &env) noexcept {
	auto ptr = std::getenv(env.c_str());
	std::string value;
	if (ptr) {
		value = std::string(ptr);
	}
	return std::move(value);
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

static std::tuple<std::string, std::string> GetCredentialsFromCephArgs(std::string_view ceph_args) {
	std::string username, password;
	auto pos = ceph_args.find("client");
	if (pos == std::string::npos) {
		throw std::runtime_error("can not find username in CEPH_ARGS environment variable");
	}

	auto space = ceph_args.find(' ', pos);
	username = ceph_args.substr(pos, space - pos);

	pos = ceph_args.find("--key");
	if (pos == std::string::npos) {
		throw std::runtime_error("can not find key in CEPH_ARGS environment variable");
	}

	pos = ceph_args.find('=', pos + 5);
	if (pos == std::string::npos) {
		throw std::runtime_error("can not find key in CEPH_ARGS environment variable");
	}

	auto key_pos = ceph_args.find_first_not_of(' ', pos + 1);
	if (key_pos == std::string::npos) {
		throw std::runtime_error("can not find key in CEPH_ARGS environment variable");
	}

	space = ceph_args.find(' ', key_pos + 1);
	password = ceph_args.substr(key_pos, space - key_pos);

	return std::make_tuple(username, password);
}

static std::string GetLineFromFile(const std::string &ceph_conf, const std::string &filter) {
	std::string buffer;
	std::ifstream conf_file(ceph_conf.c_str(), std::ifstream::in);
	if (conf_file.is_open()) {
		while (conf_file.good()) {
			getline(conf_file, buffer);
			if (buffer.find(filter) != buffer.npos) {
				break;
			}
		}
		conf_file.close();
	}

	return buffer;
}

static std::string GetValueFromLine(const std::string &line, const std::string &key) {
	std::string value;
	if (line.empty()) {
		return value;
	}

	auto pos = line.find('=');
	if (pos == std::string::npos) {
		return value;
	}

	pos = line.find_first_not_of(' ', pos + 1);
	if (pos == std::string::npos) {
		return value;
	}

	value = std::move(line.substr(pos, line.find(' ', pos) - pos));

	return value;
}

static std::string GetKeyRingFromCephConf(const std::string &ceph_conf) {
	static std::string CEPH_KEYRING([&ceph_conf] {
		auto keyring_line = GetLineFromFile(ceph_conf, "keyring");
		if (keyring_line.empty()) {
			throw std::runtime_error("can not find keyring path in " + ceph_conf);
		}

		auto keyring_file = GetValueFromLine(keyring_line, "keyring");
		if (keyring_file.empty()) {
			throw std::runtime_error("can not find keyring file in " + ceph_conf);
		}

		auto key_line = GetLineFromFile(keyring_file, "key");
		if (key_line.empty()) {
			throw std::runtime_error("can not find key in " + keyring_file);
		}

		auto password = GetValueFromLine(key_line, "key");
		if (password.empty()) {
			throw std::runtime_error("can not find key in " + keyring_file);
		}

		return password;
	}());

	return CEPH_KEYRING;
}

std::tuple<std::string, std::string> GetCredetialsFromEnv() {
	static std::tuple<std::string, std::string> CEPH_CREDENTIAL([] {
		std::string username, password;
		auto ceph_args_name = "DATA_CORE_CEPH_ARGS";
		auto ceph_args = GetEnv(ceph_args_name);
		if (ceph_args.empty()) {
			ceph_args_name = "CEPH_ARGS";
			ceph_args = GetEnv(ceph_args_name);
		}

		if (!ceph_args.empty()) {
			return GetCredentialsFromCephArgs(ceph_args);
		}

		username = GetEnv("SYS_JDFS_USERNAME");
		if (username.empty()) {
			username = GetEnv("JDFS_USERNAME");
		}

		if (username.empty()) {
			return std::make_tuple(username, password);
		}

		if (username.find("client") == username.npos) {
			throw std::runtime_error("JDFS_USERNAME should start with client prefix like client.xxx");
		}

		auto ceph_conf = GetEnv("SYS_JDFS_CONFIG_PATH");
		if (ceph_conf.empty()) {
			ceph_conf = GetEnv("JDFS_CONFIG_PATH");
		}
		if (!ceph_conf.empty()) {
			password = GetKeyRingFromCephConf(ceph_conf);
		}

		return std::make_tuple(username, password);
	}());

	return CEPH_CREDENTIAL;
}

} // namespace duckdb
