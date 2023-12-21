#include "include/ceph_connector.hpp"

#include "LRUCache11.hpp"
#include "rados/librados.hpp"
#include "radosstriper/libradosstriper.hpp"
#include "raw_ceph_connector.hpp"
#include "utils.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstring>
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

pid_t current_pid = getpid();

struct FileMetaCache {
	// to cache PARQUET footer and meta and small files(<= 1MB)
	constexpr static std::size_t READ_CACHE_LEN = 1 << 20;

	typename std::chrono::system_clock::time_point cache_time;

	// Read info
	CephStat stat;

	// The read cache. Its maximum size is given by `READ_CACHE_LEN`.
	// The read cache always contain data from the end of the file.
	std::unique_ptr<char[]> read_cache;
	std::uint64_t read_cache_start_offset;

	bool HasExpired() const noexcept {
		auto dur = std::chrono::system_clock::now() - cache_time;
		return dur.count() < 0 || dur > CACHE_VALID_DURATION;
	}
};

bool IsPrefix(const std::string &prefix, const std::string &s) {
	if (prefix.length() > s.length()) {
		return false;
	}

	return std::equal(prefix.begin(), prefix.end(), s.begin());
}

} // namespace

class CephConnector::FileIndexManager {
public:
	explicit FileIndexManager(RawCephConnector &raw) noexcept : raw {raw} {
	}

	void InsertOrUpdate(const CephPath &path, std::error_code &ec) {
		InsertOrUpdate(std::vector<CephPath> {path}, ec);
	}

	void InsertOrUpdate(const std::vector<CephPath> &paths, std::error_code &ec) {
		// Collect updates for each object directory.
		auto query_time = UtcClock::now();
		std::map<CephPath, std::map<std::string, IndexValue>, std::greater<CephPath>> updates;

		for (const auto &p : paths) {
			Path oid_path {p.path};
			auto object_name = oid_path.GetFileName();
			auto object_dir = oid_path.GetBase();
			CephPath dir_path {p.ns, object_dir.ToString()};

			updates[dir_path].emplace(std::move(object_name), IndexValue {false, query_time});
		}

		// Apply the updates.
		while (!updates.empty()) {
			auto update_entry = std::move(*updates.begin());
			updates.erase(updates.begin());

			Path index_oid {update_entry.first.path};
			index_oid.Push(CEPH_INDEX_FILE);

			CephPath index_path {update_entry.first.ns, index_oid.ToString()};
			auto recurse_parent = [this, &ec, &update_entry, &index_path]() {
				auto ret = false;
				if (!raw.RadosExist(index_path, ec) && !ec) {
					raw.RadosCreate(index_path, ec);
					ret = true;
				}
				if (ec) {
					return false;
				}

				raw.SetOmapKv<IndexValue>(index_path, update_entry.second, &IndexValue::Serialize, ec);
				if (ec) {
					return false;
				}

				return ret;
			}();
			if (ec) {
				return;
			}

			if (!recurse_parent || update_entry.first.path.empty()) {
				continue;
			}

			Path dir_path {update_entry.first.path};
			auto parent_dir = dir_path.GetBase().ToString();
			auto dir_name = dir_path.GetFileName();

			CephPath parent_dir_path {update_entry.first.ns, parent_dir};
			updates[parent_dir_path].emplace(std::move(dir_name), IndexValue {true, query_time});
		}
	}

	void Delete(const CephPath &path) {
		Path oid_path {path.path};
		Path root {"/"};
		for (; oid_path != root; oid_path.Pop()) {
			auto object_name = oid_path.GetFileName();

			auto index_oid = oid_path.GetBase();
			index_oid.Push(CEPH_INDEX_FILE);
			CephPath index_path {path.ns, index_oid.ToString()};

			std::error_code ec {};
			if (!raw.RadosExist(index_path, ec) || ec) {
				break;
			}

			std::set<std::string> delete_keys {object_name};
			raw.DeleteOmapKeys(index_path, delete_keys, ec);
			if (ec) {
				break;
			}

			if (!raw.HasOmapKeys(index_path, ec) && !ec) {
				raw.RadosDelete(index_path, ec);
			} else {
				break;
			}
		}
	}

	std::vector<std::string> ListFiles(const CephPath &prefix) {
		std::vector<std::string> files;

		std::queue<Path> pull_queue;
		pull_queue.push(Path {prefix.path}.GetBase());
		while (!pull_queue.empty()) {
			auto next_to_pull = std::move(pull_queue.front());
			pull_queue.pop();

			auto index_oid = next_to_pull / CEPH_INDEX_FILE;

			std::error_code ec;
			auto index_omap_kv =
			    raw.GetOmapKv<IndexValue>(CephPath {prefix.ns, index_oid.ToString()}, {}, &IndexValue::Deserialize, ec);
			if (ec) {
				index_omap_kv = {};
			}

			for (auto &[component, index_value] : index_omap_kv) {
				auto next_level_path = next_to_pull / component;
				auto next_level_path_str = next_level_path.ToString();
				if (!IsPrefix(prefix.path, next_level_path_str)) {
					continue;
				}

				if (index_value.is_dir) {
					pull_queue.push(std::move(next_level_path));
				} else {
					files.push_back(next_level_path.ToString());
				}
			}
		}

		return files;
	}

	std::chrono::time_point<UtcClock> QueryFileModifiedTime(const CephPath &path, std::error_code &ec) noexcept {
		Path oid_path {path.path};
		auto object_name = oid_path.GetFileName();
		auto index_path = oid_path.GetBase();
		index_path.Push(CEPH_INDEX_FILE);

		std::set<std::string> keys {object_name};
		auto index_kv =
		    raw.GetOmapKv<IndexValue>(CephPath {path.ns, index_path.ToString()}, keys, &IndexValue::Deserialize, ec);
		if (ec) {
			return {};
		}

		auto it = index_kv.find(object_name);
		if (it == index_kv.end()) {
			ec = std::error_code {ENOENT, std::system_category()};
			return {};
		}

		ec = std::error_code {};
		return it->second.modified_time;
	}

	void Refresh(const CephNamespace &ns, std::error_code &ec) {
		auto object_names = ListObjectsDirect(ns, ec);
		if (ec) {
			return;
		}

		std::vector<CephPath> object_paths;
		object_paths.reserve(object_names.size());
		for (auto &name : object_names) {
			object_paths.push_back(CephPath {ns, std::move(name)});
		}

		InsertOrUpdate(object_paths, ec);
	}

private:
	struct IndexValue {
		static IndexValue Unpack(std::uint64_t value) noexcept {
			auto is_dir = (value & 1) != 0;
			typename std::chrono::time_point<UtcClock>::duration dur {value >> 1};
			return {is_dir, std::chrono::time_point<UtcClock> {dur}};
		}

		static std::optional<IndexValue> Deserialize(const librados::bufferlist &buffer) noexcept {
			std::uint64_t packed;
			if (buffer.length() != sizeof(packed)) {
				return std::nullopt;
			}

			char *ptr = reinterpret_cast<char *>(&packed);
			for (const auto &buf : buffer.buffers()) {
				std::memcpy(ptr, buf.c_str(), buf.length());
				ptr += buf.length();
			}

			return Unpack(packed);
		}

		bool is_dir;
		std::chrono::time_point<UtcClock> modified_time;

		std::uint64_t Pack() const noexcept {
			return (static_cast<std::uint64_t>(modified_time.time_since_epoch().count()) << 1) |
			       static_cast<std::uint64_t>(is_dir);
		}

		librados::bufferlist Serialize() const noexcept {
			auto packed = Pack();

			librados::bufferlist buffer;
			buffer.append(reinterpret_cast<const char *>(&packed), sizeof(packed));

			return buffer;
		}
	};

	RawCephConnector &raw;

	std::vector<std::string> ListObjectsDirect(const CephNamespace &ns, std::error_code &ec) {
		return raw.ListFilesAndTransform(
		    ns,
		    [this, &ns](const std::string &oid, std::error_code &ec) -> std::optional<std::string> {
			    // Skip ceph index files when enumerating files.
			    constexpr size_t CEPH_INDEX_LEN = std::size(CEPH_INDEX_FILE) - 1;
			    if (oid.find(CEPH_INDEX_FILE, oid.size() - CEPH_INDEX_LEN) != std::string::npos) {
				    raw.RadosDelete(CephPath {ns, oid}, ec);
				    return std::nullopt;
			    }

			    // Skip files that are not created by libradosstriper when enumerating files.
			    constexpr std::size_t CEPH_OBJ_SUFFIX_LENGTH = std::size(CEPH_OBJ_SUFFIX) - 1;
			    auto pos = oid.size() - CEPH_OBJ_SUFFIX_LENGTH;
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
	explicit FileMetaManager(RawCephConnector &raw) noexcept : raw {raw}, enable_cache {true}, cache {1 << 12} {
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

	void ClearCache() noexcept {
		cache.clear();
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
		// INSTANCE.~CephConnector();
		// new (&INSTANCE) CephConnector {};
		INSTANCE.GetRawConnector()->Connect();
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
	    key, [&exist](const FileMetaCache &, std::error_code &) { exist = true; }, ec);

	return exist;
}

int64_t CephConnector::Read(const std::string &path, const std::string &pool, const std::string &ns,
                            std::uint64_t file_offset, char *buffer_out, std::size_t buffer_out_len) {
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

int64_t CephConnector::Write(const std::string &path, const std::string &pool, const std::string &ns,
                             const char *buffer_in, std::size_t buffer_in_len) {
	CephPath key {{pool, ns}, path};

	std::error_code ec;
	raw->Write(key, buffer_in, buffer_in_len, ec);
	if (ec) {
		return -ec.value();
	}

	// Update file index and meta.
	index_manager->InsertOrUpdate(key, ec);
	if (ec) {
		return -ec.value();
	}

	if (meta_manager->IsCacheEnabled()) {
		meta_manager->GetFileMetaAndDo(
		    key,
		    [buffer_in, buffer_in_len](FileMetaCache &c, std::error_code &) {
			    auto now = std::chrono::system_clock::now();
			    c.cache_time = now;
			    c.stat.size = buffer_in_len;
			    c.stat.last_modified = now;

			    // Update the local read cache.
			    c.read_cache_start_offset =
			        buffer_in_len >= FileMetaCache::READ_CACHE_LEN ? buffer_in_len - FileMetaCache::READ_CACHE_LEN : 0;

			    std::size_t cache_size = buffer_in_len - c.read_cache_start_offset;
			    c.read_cache.reset(new char[cache_size]);
			    std::memcpy(c.read_cache.get(), buffer_in + c.read_cache_start_offset, cache_size);
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

std::time_t CephConnector::GetLastModifiedTime(const std::string &path, const std::string &pool,
                                               const std::string &ns) {
	CephPath object_path {{pool, ns}, path};
	std::error_code ec {};

	auto modified_time = index_manager->QueryFileModifiedTime(object_path, ec);
	if (ec) {
		return -ec.value();
	}

	return std::chrono::duration_cast<std::chrono::seconds>(modified_time.time_since_epoch()).count();
}

bool CephConnector::RefreshFileIndex(const std::string &pool, const std::string &ns) {
	CephNamespace key {pool, ns};

	std::error_code ec;
	index_manager->Refresh(key, ec);
	return !ec;
}

void CephConnector::ClearCache() {
	meta_manager->ClearCache();
}

void CephConnector::DisableCache() {
	meta_manager->DisableCache();
}

} // namespace duckdb