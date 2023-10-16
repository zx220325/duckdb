#pragma once

#include "LRUCache11.hpp"
#include "duckdb/common/file_system.hpp"
#include "radosstriper/libradosstriper.hpp"
#include "tsl/htrie_map.h"

#include <array>
#include <atomic>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

struct Elem {
	std::array<char, 240> path;
	std::size_t sz;
	std::uint64_t tm;
};

static std::string getEnv(const std::string &ENV) {
	auto ptr = std::getenv(ENV.c_str());
	std::string ret;
	if (ptr) {
		ret = std::string(ptr);
	}
	return ret;
}

static std::string_view get_jfds_username() {
	static std::string JDFS_USERNAME([] {
		std::string username;
		auto ceph_args = getEnv("CEPH_ARGS");
		if (!ceph_args.empty()) {
			auto pos = ceph_args.find("client");
			if (pos == std::string::npos) {
				return username;
			}
			auto space = ceph_args.find(' ', pos);
			username = ceph_args.substr(pos, space - pos);
		} else {
			username = getEnv("SYS_JDFS_USERNAME");
			if (username.empty()) {
				username = getEnv("JDFS_USERNAME");
			}
		}
		return username;
	}());
	return JDFS_USERNAME;
}
// extern const std::string CEPH_INDEX_MQ_NAME;
// (1 << 18) * (1 << 8)  64MB in total
extern const size_t CEPH_INDEX_MQ_SIZE;

static inline void ParseUrl(std::string_view url, std::string &pool_out, std::string &ns_out, std::string &path_out) {
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

class Lock {
public:
	Lock(const std::string &path, const std::string &pool, const std::string &ns,
	     const std::shared_ptr<librados::IoCtx> &io_ctx);
	Lock(const Lock &) = delete;
	Lock &operator=(const Lock &) = delete;
	~Lock();

private:
	const std::string &path_;
	const std::shared_ptr<librados::IoCtx> &io_ctx_;
};

class CephConnector {
	struct MetaCache {
		// Read info
		int64_t length;
		time_t last_modified;
		time_t cache_time;
		int64_t buffer_start;
		// int64_t buffer_end;

		// to cache PARQUET footer and meta and very small files
		constexpr static int64_t READ_BUFFER_LEN = 1 << 15;
		std::unique_ptr<char[]> read_buffer;
	};

public:
	static CephConnector &connnector_singleton();

	int64_t Size(const std::string &path, const std::string &pool, const std::string &ns, time_t *mtm = nullptr);

	bool Exist(const std::string &path, const std::string &pool, const std::string &ns);

	int64_t Read(const std::string &path, const std::string &pool, const std::string &ns, int64_t file_offset,
	             char *buffer_out, int64_t buffer_out_len);

	int64_t Write(const std::string &path, const std::string &pool, const std::string &ns, char *buffer_in,
	              int64_t buffer_in_len, bool update = true);

	bool Delete(const std::string &path, const std::string &pool, const std::string &ns, bool update = true);

	std::vector<std::string> ListFiles(const std::string &pathprefix, const std::string &pool, const std::string &ns);

	void RefreshFileMeta(const std::string &pool, const std::string &ns);

	void PersistChangeInMessageQueueToCeph(boost::interprocess::message_queue *mq_ptr);

	void disable_cache() {
		cache_.clear();
		enable_cache_ = false;
	}

private:
	int64_t doRead(const std::string &path, const std::string &pool, const std::string &ns, int64_t file_offset,
	               char *buffer_out, int64_t buffer_out_len);

	int initMeta(const std::string &path, const std::string &pool, const std::string &ns, MetaCache* mc);

	CephConnector() {
		initialize();
	};
	~CephConnector() = default;
	CephConnector(const CephConnector &) = delete;
	CephConnector &operator=(const CephConnector &) = delete;

	void initialize();

	struct CombStriper {
		std::shared_ptr<librados::IoCtx> io_ctx;
		std::shared_ptr<libradosstriper::RadosStriper> rs;
	};

	int getCombStriper(const std::string &pool, const std::string &ns, std::shared_ptr<CombStriper> *cs);
	librados::Rados cluster;

	std::map<std::pair<std::string, std::string>, tsl::htrie_map<char, std::uint64_t>> raw_file_meta;
	std::map<std::pair<std::string, std::string>, tsl::htrie_map<char, std::uint64_t>> increment_file_meta;

	using KeyT = std::tuple<std::string, std::string, std::string>;
	lru11::Cache<KeyT, MetaCache, std::mutex> cache_ {1 << 15};

	std::mutex mtx_;
	bool enable_cache_ {true};

	static pid_t pid_;
};
} // namespace duckdb