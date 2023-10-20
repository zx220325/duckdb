#include "include/ceph_connector.hpp"

#include "LRUCache11.hpp"
#include "boost/interprocess/interprocess_fwd.hpp"
#include "rados/librados.hpp"
#include "radosstriper/libradosstriper.hpp"
#include "raw_ceph_connector.hpp"
#include "tsl/htrie_map.h"
#include "utils.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <experimental/filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <pwd.h>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <system_error>
#include <type_traits>
#include <unistd.h>
#include <unordered_map>
#include <utility>

namespace fs = std::experimental::filesystem;

#define CHECK_RETURN(expr, value)                                                                                      \
	do {                                                                                                               \
		if (expr) {                                                                                                    \
			return value;                                                                                              \
		}                                                                                                              \
	} while (0)

namespace duckdb {

namespace {

constexpr const char CEPH_INDEX_FILE[] = ".ceph_index";

constexpr std::chrono::minutes CACHE_VALID_DURATION {10};

constexpr std::size_t MQ_SIZE_THRESHOLD = CEPH_INDEX_MQ_SIZE - 1024;

pid_t current_pid = getpid();

std::optional<fs::path> GetHomeDirectory() {
	struct passwd *pw = ::getpwuid(getuid());
	fs::path homedir = pw->pw_dir;
	if (!fs::exists(homedir)) {
		return std::nullopt;
	}
	return homedir;
}

std::optional<fs::path> GetLocalIndexCacheFilePath(const CephNamespace &ns, bool create) {
	auto path_opt = GetHomeDirectory();
	if (!path_opt.has_value()) {
		return std::nullopt;
	}

	auto &path = path_opt.value();
	path /= ".datacore/.cache";
	if (!fs::exists(path)) {
		if (create) {
			fs::create_directories(path);
		} else {
			return std::nullopt;
		}
	}

	auto filename = [&ns]() {
		std::ostringstream builder;
		builder << ns.pool << "__" << ns.ns << "__index";
		return builder.str();
	}();
	path /= filename;

	return path_opt;
}

struct FileMetaCache {
	// to cache PARQUET footer and meta and very small files
	constexpr static std::size_t READ_CACHE_LEN = 1 << 15;

	typename std::chrono::system_clock::time_point cache_time;

	// Read info
	CephStat stat;

	std::unique_ptr<char[]> read_cache;
	std::uint64_t read_cache_start_offset;

	bool HasExpired() const noexcept {
		auto dur = std::chrono::system_clock::now() - cache_time;
		return dur.count() < 0 || dur > CACHE_VALID_DURATION;
	}
};

/// A timestamp value packed with a 1-bit flag.
struct PackedTimestamp {
public:
	static std::optional<PackedTimestamp> Deserialize(const librados::bufferlist &buffer) noexcept {
		if (buffer.length() != sizeof(PackedTimestamp)) {
			return std::nullopt;
		}

		auto flat_buffer = buffer.to_str();

		PackedTimestamp tm;
		std::memcpy(&tm, flat_buffer.data(), sizeof(PackedTimestamp));

		return tm;
	}

	PackedTimestamp() noexcept = default;

	PackedTimestamp(std::uint64_t tm, bool flag) noexcept : raw {(tm << 1) | flag} {
	}

	std::uint64_t GetPacked() const noexcept {
		return raw;
	}

	std::uint64_t GetTimestamp() const noexcept {
		return raw >> 1;
	}

	bool GetFlag() const noexcept {
		return raw & 1;
	}

	librados::bufferlist Serialize() const noexcept {
		librados::bufferlist buffer;
		buffer.append(const_cast<char *>(reinterpret_cast<const char *>(this)), sizeof(PackedTimestamp));
		return buffer;
	}

private:
	std::uint64_t raw;
};

class IndexDeserializer {
public:
	explicit IndexDeserializer(const char *input) noexcept : input {input} {
	}

	template <typename T, typename std::enable_if_t<std::is_trivially_copyable_v<T>> * = nullptr>
	T operator()() noexcept {
		T value;
		std::memcpy(reinterpret_cast<char *>(&value), input, sizeof(T));
		input += sizeof(T);
		return value;
	}

	void operator()(char *value_out, std::size_t value_size) noexcept {
		std::memcpy(value_out, input, value_size);
		input += value_size;
	}

private:
	const char *input;
};

std::uint64_t GetTimeNs() noexcept {
	return std::chrono::nanoseconds {std::chrono::system_clock::now().time_since_epoch()}.count();
}

void MergeIndexes(std::map<std::string, PackedTimestamp> &target,
                  const std::map<std::string, PackedTimestamp> &src) noexcept {
	for (auto it = src.begin(); it != src.end(); ++it) {
		auto target_it = target.find(it->first);
		if (target_it == target.end() || target_it->second.GetTimestamp() < it->second.GetTimestamp()) {
			target[it->first] = it->second;
		}
	}
}

void MergeIndexes(tsl::htrie_map<char, PackedTimestamp> &target,
                  const tsl::htrie_map<char, PackedTimestamp> &src) noexcept {
	std::string key_buffer;
	for (auto it = src.begin(); it != src.end(); ++it) {
		it.key(key_buffer);
		auto target_it = target.find(key_buffer);
		if (target_it == target.end() || target_it.value().GetTimestamp() < it.value().GetTimestamp()) {
			target[key_buffer] = it.value();
		}
	}
}

} // namespace

class CephConnector::FileIndexManager {
public:
	explicit FileIndexManager(RawCephConnector &raw) noexcept
	    : raw {raw}, mu {}, raw_file_meta {}, increment_file_meta {} {
	}

	void InsertOrUpdate(const CephPath &path) {
		Update(path, PackedTimestamp {GetTimeNs(), true});
	}

	void Delete(const CephPath &path) {
		Update(path, PackedTimestamp {GetTimeNs(), false});
	}

	std::vector<std::string> ListFiles(const CephPath &prefix) {
		std::unordered_map<std::string, PackedTimestamp> ret;

		auto collect = [this, &prefix, &ret](
		                   std::map<CephNamespace, tsl::htrie_map<char, PackedTimestamp>> FileIndexManager::*member) {
			std::string object_name;
			auto prefix_range = (this->*member)[prefix.ns].equal_prefix_range(prefix.path);
			for (auto it = prefix_range.first; it != prefix_range.second; it++) {
				it.key(object_name);
				auto eit = ret.find(object_name);
				if (eit == ret.end() || eit->second.GetTimestamp() < it.value().GetTimestamp()) {
					ret[object_name] = it.value();
				}
			}
		};

		// Fist collect file names from the local incremental index cache.
		bool raw_empty = [this, &collect, &prefix]() {
			std::lock_guard lk {mu};
			collect(&FileIndexManager::increment_file_meta);
			return raw_file_meta[prefix.ns].empty();
		}();
		if (raw_empty) {
			LoadIndexFromLocalCache(prefix.ns);
			PullIndexFromCeph(prefix.ns);
		}

		// Collect file names from the local copy of the index files.
		{
			std::lock_guard lk {mu};
			collect(&FileIndexManager::raw_file_meta);
		}

		std::vector<std::string> obj_list;
		for (auto &[k, v] : ret) {
			if (v.GetFlag()) {
				obj_list.push_back(k);
			}
		}
		return obj_list;
	}

	void Refresh(const CephNamespace &ns) {
		std::error_code ec;
		auto object_names = ListObjectsDirect(CephPath {ns, ""}, ec);
		if (ec) {
			return;
		}

		PackedTimestamp query_time {GetTimeNs(), true};

		// Update raw_meta.
		std::lock_guard lk {mu};

		raw_file_meta[ns].clear();
		for (std::size_t i = 0; i < object_names.size(); i++) {
			raw_file_meta[ns][object_names[i]] = query_time;
		}
		raw_file_meta[ns].shrink_to_fit();

		PushIndexToCeph(ns, raw_file_meta[ns]);
	}

	void PersistChangeInMqToCeph() {
		auto mq = OpenMq();
		if (mq.get_num_msg() > 0) {
			PersistChangeInMqToCeph(mq);
		} else {
			boost::interprocess::message_queue::remove(GetJdfsUsername().data());
		}
	}

private:
	struct MqMessage {
		MqMessage() noexcept = default;

		MqMessage(const CephPath &key, PackedTimestamp tm) : path {}, path_len {0}, tm {tm} {
			auto s = key.ToString();
			if (s.size() > path.size()) {
				throw std::runtime_error("path is to long");
			}
			std::memcpy(path.data(), s.data(), s.size());
			path_len = s.size();
		}

		std::array<char, 240> path;
		std::size_t path_len;
		PackedTimestamp tm;
	};

	static boost::interprocess::message_queue OpenMq() {
		return boost::interprocess::message_queue {boost::interprocess::open_or_create, GetJdfsUsername().data(),
		                                           CEPH_INDEX_MQ_SIZE, sizeof(MqMessage)};
	}

	RawCephConnector &raw;
	std::mutex mu;
	std::map<CephNamespace, tsl::htrie_map<char, PackedTimestamp>> raw_file_meta;
	std::map<CephNamespace, tsl::htrie_map<char, PackedTimestamp>> increment_file_meta;

	void PersistChangeInMqToCeph(boost::interprocess::message_queue &mq) {
		if (mq.get_num_msg() == 0) {
			return;
		}

		MqMessage elem;
		std::size_t recv_size;
		unsigned int pq;

		std::map<CephNamespace, tsl::htrie_map<char, PackedTimestamp>> mp;
		while (mq.try_receive(&elem, sizeof(elem), recv_size, pq)) {
			auto url = std::string_view(elem.path.data(), elem.path_len);
			std::string path, pool, ns;
			ParseUrl(url, pool, ns, path);

			if (path == CEPH_INDEX_FILE) {
				continue;
			}

			CephNamespace key {pool, ns};
			auto it = mp[key].find(path);
			if (it == mp[key].end() || it.value().GetTimestamp() < elem.tm.GetTimestamp()) {
				mp[key][path] = elem.tm;
			}
		}

		boost::interprocess::message_queue::remove(GetJdfsUsername().data());

		for (auto &[ns, index] : mp) {
			PushIndexToCeph(ns, index);
		}
	}

	void Update(const CephPath &path, PackedTimestamp tm) {
		auto mq = OpenMq();
		if (mq.get_num_msg() > MQ_SIZE_THRESHOLD) {
			PersistChangeInMqToCeph(mq);
		}

		MqMessage msg {path, tm};
		mq.send(&msg, sizeof(msg), 0);

		std::lock_guard lk {mu};
		increment_file_meta[path.ns][path.path] = tm;
	}

	void PushIndexToCeph(const CephNamespace &ns, const tsl::htrie_map<char, PackedTimestamp> &index) {
		// Collect updates for each index file.
		std::unordered_map<std::string, std::map<std::string, PackedTimestamp>> updates;

		std::string oid;
		for (auto it = index.begin(); it != index.end(); ++it) {
			it.key(oid);
			auto tm = it.value();

			fs::path oid_path {oid};
			auto oid_root = oid_path.root_path();

			do {
				auto component_name = oid_path.filename();
				auto parent = oid_path.parent_path();
				auto index_path = parent / CEPH_INDEX_FILE;

				updates[index_path.string()].emplace(component_name.string(), tm);

				oid_path = std::move(parent);
			} while (oid_path != oid_root);
		}

		// Apply the updates.
		for (const auto &[index_oid, kv_updates] : updates) {
			CephPath index_path {ns, index_oid};

			std::set<std::string> update_keys;
			for (const auto &[k, v] : kv_updates) {
				update_keys.insert(k);
			}

			std::error_code ec;
			auto existing_kv =
			    raw.GetOmapKv<PackedTimestamp>(index_path, update_keys, &PackedTimestamp::Deserialize, ec);
			if (ec) {
				continue;
			}

			MergeIndexes(existing_kv, kv_updates);

			raw.SetOmapKv<PackedTimestamp>(
			    index_path, existing_kv, [](const PackedTimestamp &tm) { return tm.Serialize(); }, ec);
		}
	}

	void PullIndexFromCeph(const CephNamespace &ns) {
		tsl::htrie_map<char, PackedTimestamp> pulled_index;

		std::queue<fs::path> pull_queue;
		pull_queue.emplace("/");
		while (!pull_queue.empty()) {
			auto next_to_pull = std::move(pull_queue.front());
			pull_queue.pop();

			auto index_oid = next_to_pull / CEPH_INDEX_FILE;

			std::error_code ec;
			auto index_omap_kv =
			    raw.GetOmapKv<PackedTimestamp>(CephPath {ns, index_oid}, {}, &PackedTimestamp::Deserialize, ec);
			if (ec) {
				index_omap_kv = {};
			}

			if (index_omap_kv.empty()) {
				auto object_names = ListObjectsDirect(CephPath {ns, next_to_pull.string()}, ec);
				if (ec) {
					continue;
				}
				PackedTimestamp query_time {GetTimeNs(), true};
				for (auto &name : object_names) {
					auto oid_path = next_to_pull / name;
					pulled_index[oid_path.string()] = query_time;
				}
				continue;
			}

			for (auto &[component, tm] : index_omap_kv) {
				auto next_level_path = next_to_pull / component;
				if (raw.Exist(CephPath {ns, next_level_path.string()}, ec) && !ec) {
					pulled_index[next_level_path.string()] = tm;
					continue;
				}
				if (ec) {
					continue;
				}

				pull_queue.push(std::move(next_level_path));
			}
		}

		std::lock_guard lk {mu};
		MergeIndexes(raw_file_meta[ns], pulled_index);
	}

	void LoadIndexFromLocalCache(const CephNamespace &ns) {
		auto local_cache = GetLocalIndexCacheFilePath(ns, false);
		if (!local_cache.has_value()) {
			return;
		}

		auto local_cache_buffer = [&local_cache]() {
			std::ifstream fin {local_cache.value()};
			std::stringstream buf;
			buf << fin.rdbuf();
			return buf.str();
		}();

		IndexDeserializer deserializer {local_cache_buffer.c_str()};
		auto tmp = tsl::htrie_map<char, PackedTimestamp>::deserialize(deserializer);
		auto &target = raw_file_meta[ns];

		std::lock_guard lk {mu};
		std::string key_buffer;
		for (auto it = tmp.begin(); it != tmp.end(); ++it) {
			it.key(key_buffer);
			auto mit = target.find(key_buffer);
			if (mit == target.end() || mit.value().GetTimestamp() < it.value().GetTimestamp()) {
				target[key_buffer] = it.value();
			}
		}
	}

	std::vector<std::string> ListObjectsDirect(const CephPath &prefix, std::error_code &ec) {
		return raw.ListFilesAndTransform(
		    prefix.ns,
		    [](const std::string &oid, std::error_code &ec) -> std::optional<std::string> {
			    constexpr std::size_t CEPH_OBJ_SUFFIX_LENGTH = std::size(CEPH_OBJ_SUFFIX) - 1;
			    if (oid.size() < CEPH_OBJ_SUFFIX_LENGTH) {
				    return std::nullopt;
			    }

			    const auto pos = oid.size() - CEPH_OBJ_SUFFIX_LENGTH;
			    if (oid.find(CEPH_OBJ_SUFFIX, pos) != std::string::npos) {
				    return oid.substr(0, pos);
			    }

			    return std::nullopt;
		    },
		    ec);
	}
};

class CephConnector::FileMetaManager {
public:
	explicit FileMetaManager(RawCephConnector &raw) noexcept : raw {raw}, enable_cache {true}, cache {1 << 15} {
	}

	template <typename FN>
	void GetFileMetaAndDo(const CephPath &path, FN callback, std::error_code &ec) {
		static_assert(std::is_invocable_r_v<void, FN, FileMetaCache &, std::error_code &>);

		if (enable_cache) {
			auto cache_hit = cache.tryOperate(path, [&callback, &ec](FileMetaCache &c) {
				if (c.HasExpired()) {
					return false;
				}
				ec = std::error_code {};
				callback(c, ec);
				return true;
			});
			if (cache_hit) {
				return;
			}
		}

		auto stat = raw.Stat(path, ec);
		if (ec) {
			return;
		}

		FileMetaCache meta_cache {std::chrono::system_clock::now(), stat};
		ec = std::error_code {};
		callback(meta_cache, ec);

		if (enable_cache) {
			cache.insert(path, std::move(meta_cache));
		}
	}

	void DeleteCacheOfFile(const CephPath &path) {
		if (!enable_cache) {
			return;
		}
		cache.remove(path);
	}

	void DisableCache() noexcept {
		enable_cache = false;
		cache.clear();
	}

	bool IsCacheEnabled() const noexcept {
		return enable_cache;
	}

private:
	RawCephConnector &raw;
	bool enable_cache;
	lru11::Cache<CephPath, FileMetaCache, std::mutex> cache;
};

CephConnector &CephConnector::GetSingleton() {
	static CephConnector INSTANCE;

	auto cur_pid = ::getpid();

	// Detect fork.
	if (cur_pid != current_pid) {
		current_pid = cur_pid;

		// After forking, re-initialize the global CephConnector object.
		INSTANCE.~CephConnector();
		new (&INSTANCE) CephConnector {};
	}

	return INSTANCE;
}

CephConnector::CephConnector()
    : raw {std::make_unique<RawCephConnector>()}, index_manager {std::make_unique<FileIndexManager>(*raw)},
      meta_manager {std::make_unique<FileMetaManager>(*raw)} {
}

CephConnector::~CephConnector() noexcept = default;

int64_t CephConnector::Size(const std::string &path, const std::string &pool, const std::string &ns, time_t *mtm) {
	CephPath key {{pool, ns}, path};
	std::error_code ec;

	std::size_t size = 0;
	meta_manager->GetFileMetaAndDo(
	    key,
	    [mtm, &size](const FileMetaCache &c, std::error_code &) {
		    size = c.stat.size;
		    if (mtm) {
			    *mtm =
			        std::chrono::duration_cast<std::chrono::seconds>(c.stat.last_modified.time_since_epoch()).count();
		    }
	    },
	    ec);

	if (ec) {
		return -ec.value();
	}

	return size;
}

bool CephConnector::Exist(const std::string &path, const std::string &pool, const std::string &ns) {
	CephPath key {{pool, ns}, path};
	std::error_code ec;

	auto exist = false;
	meta_manager->GetFileMetaAndDo(
	    key, [&exist](const FileMetaCache &c, std::error_code &) { exist = c.stat.size > 0; }, ec);

	return exist;
}

int64_t CephConnector::Read(const std::string &path, const std::string &pool, const std::string &ns,
                            std::uint64_t file_offset, char *buffer_out, std::size_t buffer_out_len) {
	// return doRead(path, pool, ns, file_offset, buffer_out, buffer_out_len);
	auto raw_sz = Size(path, pool, ns);
	CHECK_RETURN(raw_sz <= 0, raw_sz);

	std::size_t sz = raw_sz;

	CephPath key {{pool, ns}, path};
	auto to_read = buffer_out_len;

	std::error_code ec;
	meta_manager->GetFileMetaAndDo(
	    key,
	    [&file_offset, &buffer_out_len, &buffer_out, &to_read](const FileMetaCache &mc, std::error_code &ec) {
		    if (mc.read_cache && file_offset + buffer_out_len > mc.read_cache_start_offset) {
			    // The read cache is not empty and contains part of the data that we currently want. Try to read some
			    // data from the read cache.

			    // Note that the read cache in the FileMetaCache always contain data from the end of the file.
			    auto can_read = std::min(file_offset + buffer_out_len - mc.read_cache_start_offset, buffer_out_len);
			    auto buffer_offset =
			        file_offset >= mc.read_cache_start_offset ? file_offset - mc.read_cache_start_offset : 0;
			    std::memcpy(buffer_out + buffer_out_len - can_read, mc.read_cache.get() + buffer_offset, can_read);
			    to_read -= can_read;
		    }
	    },
	    ec);
	if (ec) {
		return -ec.value();
	}
	if (to_read == 0) {
		return buffer_out_len;
	}

	// Special optimization for parquet or very small files (usually some meta files).
	if (meta_manager->IsCacheEnabled() &&
	    (sz <= FileMetaCache::READ_CACHE_LEN || (buffer_out_len == 8 && file_offset + 8 == sz))) {
		auto can_read = std::min<std::size_t>(FileMetaCache::READ_CACHE_LEN, sz);
		std::unique_ptr<char[]> cache {new char[can_read]};
		auto can_read_offset = sz - can_read;

		raw->Read(key, can_read_offset, cache.get(), can_read, ec);
		if (ec) {
			return -ec.value();
		}

		std::memcpy(buffer_out, cache.get() + can_read - buffer_out_len, buffer_out_len);

		meta_manager->GetFileMetaAndDo(
		    key,
		    [&cache, &can_read_offset](FileMetaCache &c, std::error_code &) {
			    c.read_cache = std::move(cache);
			    c.read_cache_start_offset = can_read_offset;
		    },
		    ec);
		if (ec) {
			return -ec.value();
		}

		return buffer_out_len;
	}

	auto ret = raw->Read(key, file_offset, buffer_out, to_read, ec);
	if (ec) {
		return -ec.value();
	}

	return ret;
}

int64_t CephConnector::Write(const std::string &path, const std::string &pool, const std::string &ns, char *buffer_in,
                             std::size_t buffer_in_len) {
	CephPath key {{pool, ns}, path};

	std::error_code ec;
	raw->Write(key, buffer_in, buffer_in_len, ec);
	if (ec) {
		return -ec.value();
	}

	// Update file index and meta.
	index_manager->InsertOrUpdate(key);
	if (meta_manager->IsCacheEnabled()) {
		meta_manager->GetFileMetaAndDo(
		    key,
		    [&buffer_in_len](FileMetaCache &c, std::error_code &) {
			    auto now = std::chrono::system_clock::now();
			    c.cache_time = now;
			    c.stat.size = buffer_in_len;
			    c.stat.last_modified = now;
		    },
		    ec);
		if (ec) {
			return -ec.value();
		}
	}

	return buffer_in_len;
}

bool CephConnector::Delete(const std::string &path, const std::string &pool, const std::string &ns) {
	CephPath key {{pool, ns}, path};
	std::error_code ec;

	raw->Delete(key, ec);
	if (ec) {
		return false;
	}

	// Update file index and file meta.
	index_manager->Delete(key);
	meta_manager->DeleteCacheOfFile(key);

	return true;
}

std::vector<std::string> CephConnector::ListFiles(const std::string &path, const std::string &pool,
                                                  const std::string &ns) {
	CephPath prefix {{pool, ns}, path};
	return index_manager->ListFiles(prefix);
}

void CephConnector::RefreshFileIndex(const std::string &pool, const std::string &ns) {
	CephNamespace key {pool, ns};
	index_manager->Refresh(key);
}

void CephConnector::PersistChangeInMessageQueueToCeph() {
	index_manager->PersistChangeInMqToCeph();
}

void CephConnector::DisableCache() {
	meta_manager->DisableCache();
}

} // namespace duckdb