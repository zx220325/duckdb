#pragma once

#include "LRUCache11.hpp"
#include "boost/interprocess/ipc/message_queue.hpp"
#include "duckdb/common/file_system.hpp"
#include "radosstriper/libradosstriper.hpp"
#include "tsl/htrie_map.h"

#include <array>
#include <atomic>
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

// extern const std::string CEPH_INDEX_MQ_NAME;
// (1 << 18) * (1 << 8)  64MB in total
extern const size_t CEPH_INDEX_MQ_SIZE;

class Lock {
public:
	Lock(const std::string &path, const std::string &pool, const std::string &ns,
	     const std::shared_ptr<librados::IoCtx> &io_ctx);
	Lock(const Lock &) = delete;
	Lock &operator=(const Lock &) = delete;
	~Lock();

private:
	const std::string &path;
	const std::shared_ptr<librados::IoCtx> &io_ctx;
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
	static CephConnector &GetSingleton();

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

	void DisableCache() {
		cache.clear();
		enable_cache = false;
	}

private:
	int64_t DoRead(const std::string &path, const std::string &pool, const std::string &ns, int64_t file_offset,
	               char *buffer_out, int64_t buffer_out_len);

	int InitMeta(const std::string &path, const std::string &pool, const std::string &ns, MetaCache *mc);

	CephConnector() {
		Initialize();
	};
	~CephConnector() = default;
	CephConnector(const CephConnector &) = delete;
	CephConnector &operator=(const CephConnector &) = delete;

	void Initialize();

	struct CombStriper {
		std::shared_ptr<librados::IoCtx> io_ctx;
		std::shared_ptr<libradosstriper::RadosStriper> rs;
	};

	int GetCombStriper(const std::string &pool, const std::string &ns, std::shared_ptr<CombStriper> *cs);
	librados::Rados cluster;

	std::map<std::pair<std::string, std::string>, tsl::htrie_map<char, std::uint64_t>> raw_file_meta;
	std::map<std::pair<std::string, std::string>, tsl::htrie_map<char, std::uint64_t>> increment_file_meta;

	using KeyT = std::tuple<std::string, std::string, std::string>;
	lru11::Cache<KeyT, MetaCache, std::mutex> cache {1 << 15};

	std::mutex mtx;
	bool enable_cache {true};

	static pid_t pid;
};
} // namespace duckdb