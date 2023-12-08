#pragma once

#include "rados/librados.hpp"

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace duckdb {

constexpr const char CEPH_OBJ_SUFFIX[] = ".0000000000000000";

/// Identifies a namespace in the ceph cluster.
struct CephNamespace {
	std::string pool;
	std::string ns;

	std::size_t GetHashCode() const noexcept;

	friend bool operator==(const CephNamespace &lhs, const CephNamespace &rhs) noexcept;
	friend bool operator!=(const CephNamespace &lhs, const CephNamespace &rhs) noexcept;

	// The comparison operators below defines an opaque total ordering among all possible CephNamespace objects. The
	// defined total ordering may not carry any meaningful semantics.
	friend bool operator<(const CephNamespace &lhs, const CephNamespace &rhs) noexcept;
	friend bool operator<=(const CephNamespace &lhs, const CephNamespace &rhs) noexcept;
	friend bool operator>=(const CephNamespace &lhs, const CephNamespace &rhs) noexcept;
	friend bool operator>(const CephNamespace &lhs, const CephNamespace &rhs) noexcept;
};

/// Path to an object in the ceph cluster.
struct CephPath {
	CephNamespace ns;
	std::string path;

	std::size_t GetHashCode() const noexcept;

	std::string ToString() const noexcept;

	friend bool operator==(const CephPath &lhs, const CephPath &rhs) noexcept;
	friend bool operator!=(const CephPath &lhs, const CephPath &rhs) noexcept;

	// The comparison operators below defines an opaque total ordering among all possible CephPath objects. The defined
	// total ordering may not carry any meaningful semantics.
	friend bool operator<(const CephPath &lhs, const CephPath &rhs) noexcept;
	friend bool operator<=(const CephPath &lhs, const CephPath &rhs) noexcept;
	friend bool operator>=(const CephPath &lhs, const CephPath &rhs) noexcept;
	friend bool operator>(const CephPath &lhs, const CephPath &rhs) noexcept;
};

/// Status of a ceph object.
struct CephStat {
	std::uint64_t size;
	typename std::chrono::system_clock::time_point last_modified;
};

/// Access ceph cluster without any caches.
class RawCephConnector {
public:
	RawCephConnector();

	RawCephConnector(const RawCephConnector &) = delete;
	RawCephConnector(RawCephConnector &&) = delete;

	~RawCephConnector() noexcept = default;

	void Connect();

	RawCephConnector &operator=(const RawCephConnector &) = delete;
	RawCephConnector &operator=(RawCephConnector &&) = delete;

	CephStat Stat(const CephPath &path, std::error_code &ec) noexcept;

	bool Exist(const CephPath &path, std::error_code &ec) noexcept;

	bool RadosExist(const CephPath &path, std::error_code &ec) noexcept;

	void RadosCreate(const CephPath &path, std::error_code &ec) noexcept;

	std::size_t Read(const CephPath &path, std::uint64_t file_offset, void *buffer, std::size_t buffer_size,
	                 std::error_code &ec) noexcept;

	std::size_t Write(const CephPath &path, const void *buffer, std::size_t buffer_size, std::error_code &ec) noexcept;

	void Delete(const CephPath &path, std::error_code &ec) noexcept;

	void RadosDelete(const CephPath &path, std::error_code &ec) noexcept;

	std::vector<std::string> ListFiles(const CephNamespace &ns, std::error_code &ec) noexcept;

	std::vector<std::string>
	ListFilesAndFilter(const CephNamespace &ns,
	                   const std::function<bool(const std::string &, std::error_code &)> &predicate,
	                   std::error_code &ec);

	std::vector<std::string>
	ListFilesAndTransform(const CephNamespace &ns,
	                      const std::function<std::optional<std::string>(std::string, std::error_code &)> &transform,
	                      std::error_code &ec);

	std::map<std::string, librados::bufferlist> GetOmapKv(const CephPath &path, const std::set<std::string> &keys,
	                                                      std::error_code &ec) noexcept;

	template <typename T>
	std::map<std::string, T>
	GetOmapKv(const CephPath &path, const std::set<std::string> &keys,
	          const std::function<std::optional<T>(const librados::bufferlist &)> &deserializer,
	          std::error_code &ec) noexcept {
		auto raw_kv = GetOmapKv(path, keys, ec);
		if (ec) {
			return {};
		}

		std::map<std::string, T> ret;
		for (auto &[key, value_data] : raw_kv) {
			auto value = deserializer(value_data);
			if (!value.has_value()) {
				ec = std::error_code {EINVAL, std::system_category()};
			}

			ret.emplace(key, std::move(value.value()));
		}

		return ret;
	}

	void SetOmapKv(const CephPath &path, const std::map<std::string, librados::bufferlist> &kv,
	               std::error_code &ec) noexcept;

	template <typename T>
	void SetOmapKv(const CephPath &path, const std::map<std::string, T> &kv,
	               const std::function<librados::bufferlist(const T &)> &serializer, std::error_code &ec) noexcept {
		std::map<std::string, librados::bufferlist> raw_kv;
		for (const auto &[key, value] : kv) {
			auto value_data = serializer(value);
			raw_kv.emplace(key, std::move(value_data));
		}

		SetOmapKv(path, raw_kv, ec);
	}

	void DeleteOmapKeys(const CephPath &path, const std::set<std::string> &keys, std::error_code &ec) noexcept;

	bool HasOmapKeys(const CephPath &path, std::error_code &ec) noexcept;

private:
	struct RadosContext;
	void BusyWaitBreakRetry(const CephPath &path, const RadosContext &ctx,
	                      std::function<int(const CephPath &, const RadosContext &)> func, std::error_code &ec) noexcept;

	librados::Rados cluster;
};

} // namespace duckdb

namespace std {

template <>
struct hash<duckdb::CephNamespace> {
	std::size_t operator()(const duckdb::CephNamespace &ns) noexcept {
		return ns.GetHashCode();
	}
};

template <>
struct hash<duckdb::CephPath> {
	std::size_t operator()(const duckdb::CephPath &path) noexcept {
		return path.GetHashCode();
	}
};

} // namespace std
