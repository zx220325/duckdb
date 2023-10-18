#include "include/ceph_connector.hpp"

#include "radosstriper/libradosstriper.hpp"
#include "utils.hpp"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <chrono>
#include <cstring>
#include <ctime>
#include <experimental/filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <pwd.h>
#include <random>
#include <sstream>
#include <string>
#include <sys/types.h>
#include <unistd.h>

namespace duckdb {

#define CHECK_RETURN(expr, value)                                                                                      \
	do {                                                                                                               \
		if (expr) {                                                                                                    \
			return value;                                                                                              \
		}                                                                                                              \
	} while (0)

// (1 << 18) * (1 << 8)  64MB in total
const size_t CEPH_INDEX_MQ_SIZE = 1 << 18;
static const std::string CEPH_INDEX_FILE = ".ceph_index";
static const size_t MQ_SIZE_THRESHOLD = CEPH_INDEX_MQ_SIZE - 1024;

static constexpr int64_t SPLIT_SIZE = 1024 * 1024 * 1024;
static constexpr int64_t VALID_DURATION = 600000; // 10min

static const std::string LOCK_NAME = "default_lock";
Lock::Lock(const std::string &path, const std::string &pool, const std::string &ns,
           const std::shared_ptr<librados::IoCtx> &io_ctx)
    : path(path), io_ctx(io_ctx) {
	io_ctx->lock_exclusive(path, LOCK_NAME, "", "", nullptr, 0);
}

Lock::~Lock() {
	io_ctx->unlock(path, LOCK_NAME, "");
}

static constexpr const char *CLUSTER_NAME = "ceph";
pid_t CephConnector::pid = getpid();

namespace fs = std::experimental::filesystem;

static inline std::string GetLocalCache(const std::string &pool, const std::string &ns, bool create) {
	struct passwd *pw = getpwuid(getuid());
	std::string homedir = pw->pw_dir;
	if (!fs::exists(homedir)) {
		return "";
	}
	std::string dir = homedir + "/.data_core/.cache/";
	if (!fs::exists(dir) && create) {
		fs::create_directories(dir);
	}
	return dir + pool + "__" + ns + "__index";
}

static Elem Conv2Elem(const std::string &path, const std::string &pool, const std::string &ns, std::uint64_t tm) {
	auto s = "ceph://" + pool + "//" + ns + "//" + path;
	Elem elem;
	if (s.size() + 16 > sizeof(Elem)) {
		throw std::runtime_error("path is to long");
	}
	elem.sz = s.size();
	memcpy(elem.path.data(), s.data(), s.size());
	elem.tm = tm;
	return elem;
}

static std::int64_t GetTimeNs() {
	auto tm = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::nanoseconds>(tm.time_since_epoch()).count();
}

static std::int64_t GetTimeMs() {
	auto tm = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::milliseconds>(tm.time_since_epoch()).count();
}

static const std::string CEPH_OBJ_SUFFIX = ".0000000000000000";

CephConnector &CephConnector::GetSingleton() {
	static CephConnector INSTANCE;

	auto cur_pid = getpid();
	// detecting fork
	if (cur_pid != pid) {
		INSTANCE.Initialize();
		pid = cur_pid;
	}

	return INSTANCE;
}

void CephConnector::Initialize() {
	int err;
	if (GetJdfsUsername().empty()) {
		throw std::runtime_error("can not find JDFS_USERNAME in environment variable");
	}
	auto ceph_args = GetEnv("CEPH_ARGS");
	if (!ceph_args.empty()) {
		err = cluster.init2(GetJdfsUsername().data(), nullptr, 0);
		if (err < 0) {
			throw std::runtime_error(std::string("Couldn't init cluster ") + strerror(-err));
		}
		err = cluster.conf_parse_env("CEPH_ARGS");
		if (err < 0) {
			throw std::runtime_error(std::string("Couldn't parse config ") + strerror(-err));
		}
	} else {
		std::string config_path = GetEnv("SYS_JDFS_CONFIG_PATH");
		if (config_path.empty()) {
			config_path = GetEnv("JDFS_CONFIG_PATH");
		}
		if (config_path.empty()) {
			throw std::runtime_error("Environment Variable JDFS_CONFIG_PATH was not found!");
		}
		err = cluster.init2(GetJdfsUsername().data(), CLUSTER_NAME, 0);
		if (err < 0) {
			throw std::runtime_error(std::string("Couldn't init cluster ") + strerror(-err));
		}
		err = cluster.conf_read_file(config_path.c_str());
		if (err < 0) {
			throw std::runtime_error(std::string("Couldn't read conf file ") + strerror(-err));
		}
	}
	err = cluster.connect();
	if (err < 0) {
		throw std::runtime_error(std::string("Couldn't connect to cluster ") + strerror(-err));
	}
}

int CephConnector::InitMeta(const std::string &path, const std::string &pool, const std::string &ns,
                            CephConnector::MetaCache *mc) {
	auto combrs = std::make_shared<CombStriper>();
	auto ret = this->GetCombStriper(pool, ns, &combrs);
	if (ret < 0) {
		mc->length = mc->last_modified = ret;
		return ret;
	}
	uint64_t size = 0;
	timespec tsp {};
	combrs->rs->stat2(path, &size, &tsp);
	if (size == 0) {
		ceph::bufferlist bufferlist;
		ret = combrs->io_ctx->getxattr(path + CEPH_OBJ_SUFFIX, "striper.layout.object_size", bufferlist);
		if (ret < 0) {
			size = ret;
		}
	}
	mc->length = size;
	mc->last_modified = tsp.tv_sec;
	mc->cache_time = GetTimeMs();
	return ret;
}

int64_t CephConnector::Size(const std::string &path, const std::string &pool, const std::string &ns, time_t *mtm) {

	auto &&key = std::forward_as_tuple(path, pool, ns);
	int64_t ret;
	if (enable_cache && cache.tryOperate(key, [&ret, &mtm](MetaCache &mc) {
		    if (mc.cache_time + VALID_DURATION < GetTimeMs()) {
			    return false;
		    }
		    if (mtm) {
			    *mtm = mc.last_modified;
		    }
		    ret = mc.length;
		    return true;
	    })) {
		return ret;
	}
	MetaCache cc;
	InitMeta(path, pool, ns, &cc);
	if (mtm) {
		*mtm = cc.last_modified;
	}
	ret = cc.length;
	if (enable_cache) {
		cache.insert(key, std::move(cc));
	}

	return ret;
}

bool CephConnector::Exist(const std::string &path, const std::string &pool, const std::string &ns) {
	auto &&key = std::forward_as_tuple(path, pool, ns);
	bool exist;
	if (enable_cache && cache.tryOperate(key, [&exist](MetaCache &mc) {
		    if (mc.cache_time + VALID_DURATION < GetTimeMs()) {
			    return false;
		    }
		    exist = mc.length >= 0;
		    return true;
	    })) {
		return exist;
	}
	MetaCache cc;
	InitMeta(path, pool, ns, &cc);
	exist = cc.length >= 0;
	if (enable_cache) {
		cache.insert(key, std::move(cc));
	}
	return exist;
}

int64_t CephConnector::DoRead(const std::string &path, const std::string &pool, const std::string &ns,
                              int64_t file_offset, char *buffer_out, int64_t buffer_out_len) {
	auto combrs = std::make_shared<CombStriper>();
	auto ret = GetCombStriper(pool, ns, &combrs);
	CHECK_RETURN(ret < 0, ret);
	int64_t has_read = 0;
	// issue: https://stackoverflow.com/questions/70368651/why-cant-linux-write-more-than-2147479552-bytes
	for (int64_t offset = 0; offset < buffer_out_len; offset += SPLIT_SIZE) {
		ceph::bufferlist bl;
		auto ret = combrs->rs->read(path, &bl, std::min(SPLIT_SIZE, buffer_out_len - offset), offset + file_offset);
		CHECK_RETURN(ret <= 0, ret);
		for (auto &buf : bl.buffers()) {
			memcpy(buffer_out + has_read, buf.c_str(), buf.length());
			has_read += buf.length();
		}
	}
	return has_read;
}

int64_t CephConnector::Read(const std::string &path, const std::string &pool, const std::string &ns,
                            int64_t file_offset, char *buffer_out, int64_t buffer_out_len) {
	// return doRead(path, pool, ns, file_offset, buffer_out, buffer_out_len);
	auto sz = Size(path, pool, ns);
	CHECK_RETURN(sz <= 0, sz);
	auto &&key = std::forward_as_tuple(path, pool, ns);
	int64_t buffer_offset = 0;
	auto to_read = buffer_out_len;
	if (enable_cache && cache.tryOperate(key, [&file_offset, &buffer_out_len, &buffer_out, &to_read](MetaCache &cc) {
		    if (cc.cache_time + VALID_DURATION < GetTimeMs()) {
			    return false;
		    }
		    if (cc.read_buffer && file_offset + buffer_out_len > cc.buffer_start) {
			    auto can_read = std::min(file_offset + buffer_out_len - cc.buffer_start, buffer_out_len);
			    auto buffer_offset = std::max(0L, file_offset - cc.buffer_start);
			    std::memcpy(buffer_out + buffer_out_len - can_read, cc.read_buffer.get() + buffer_offset, can_read);
			    to_read -= can_read;
			    return true;
		    }
		    return false;
	    })) {
		if (to_read == 0) {
			return buffer_out_len;
		}
	}
	// special optimize for parquet or very small files(usually some meta files)
	if (enable_cache && (sz <= MetaCache::READ_BUFFER_LEN || (buffer_out_len == 8 && file_offset + 8 == sz))) {
		auto can_read = std::min(MetaCache::READ_BUFFER_LEN, sz);
		auto tmp = std::make_unique<char[]>(can_read);
		auto can_read_offset = sz - can_read;
		auto ret = DoRead(path, pool, ns, can_read_offset, tmp.get(), can_read);
		CHECK_RETURN(ret < 0, ret);
		std::memcpy(buffer_out, tmp.get() + can_read - buffer_out_len, buffer_out_len);
		cache.tryOperate(key, [&tmp, &can_read_offset](MetaCache &cc) {
			cc.buffer_start = can_read_offset;
			cc.read_buffer = std::move(tmp);
			return true;
		});
	} else {
		auto ret = DoRead(path, pool, ns, file_offset, buffer_out + buffer_offset, to_read);
		CHECK_RETURN(ret < 0, ret);
	}
	return buffer_out_len;
}

int64_t CephConnector::Write(const std::string &path, const std::string &pool, const std::string &ns, char *buffer_in,
                             int64_t buffer_in_len, bool update) {
	CHECK_RETURN(Exist(path, pool, ns) && !Delete(path, pool, ns, false), -1);
	auto combrs = std::make_shared<CombStriper>();
	auto ret = GetCombStriper(pool, ns, &combrs);
	CHECK_RETURN(ret < 0, ret);
	std::vector<std::unique_ptr<librados::AioCompletion>> completions;
	int64_t transmited = 0;
	for (; transmited < buffer_in_len; transmited += SPLIT_SIZE) // create write ops
	{
		const auto transfer_size = std::min(SPLIT_SIZE, buffer_in_len - transmited);
		auto bl = ceph::bufferlist::static_from_mem(const_cast<char *>(buffer_in) + transmited, transfer_size);
		completions.emplace_back(librados::Rados::aio_create_completion());
		auto ret = combrs->rs->aio_write(path, completions.back().get(), bl, transfer_size, transmited);
		CHECK_RETURN(ret < 0, ret);
	}
	// wait for all job complete
	for (auto &complete : completions) {
		int ret = 0;
		if (!complete->is_complete()) {
			ret = complete->wait_for_complete();
		}
		CHECK_RETURN(ret != 0, ret);
		ret = complete->get_return_value();
		CHECK_RETURN(ret != 0, ret);
	}
	{
		// set obj size, to make compatiable with jdfs;
		ceph::bufferlist bl;
		bl.append("0.1");
		int ret = combrs->rs->setxattr(path, "_version", bl);
		CHECK_RETURN(ret != 0, ret);
		bl.clear();
		bl.append(std::to_string(buffer_in_len));
		ret = combrs->rs->setxattr(path, "_size", bl);
		CHECK_RETURN(ret != 0, ret);
	}

	// update file meta
	if (update) {

		auto key = std::make_pair(pool, ns);
		auto tm = (GetTimeNs() << 1) | 1;
		boost::interprocess::message_queue mq(boost::interprocess::open_or_create, GetJdfsUsername().data(),
		                                      CEPH_INDEX_MQ_SIZE, sizeof(Elem));
		if (mq.get_num_msg() > MQ_SIZE_THRESHOLD) {
			PersistChangeInMessageQueueToCeph(&mq);
		}
		auto elem = Conv2Elem(path, pool, ns, tm);
		mq.send(&elem, sizeof(elem), 0);
		{
			std::unique_lock<std::mutex> lk(mtx);
			increment_file_meta[key][path] = tm;
		}
	}
	if (enable_cache) {
		auto &&key = std::forward_as_tuple(path, pool, ns);
		cache.tryOperate(key, [&buffer_in_len](MetaCache &cc) {
			cc.length = cc.buffer_start = buffer_in_len;
			cc.last_modified = cc.cache_time = GetTimeMs();
			return true;
		});
	}
	return buffer_in_len;
}

bool CephConnector::Delete(const std::string &path, const std::string &pool, const std::string &ns, bool update) {
	auto combrs = std::make_shared<CombStriper>();
	auto ret = GetCombStriper(pool, ns, &combrs);
	CHECK_RETURN(ret < 0, ret);
	combrs->rs->rmxattr(path, "_version");
	combrs->rs->rmxattr(path, "_size");
	ret = combrs->rs->remove(path);
	if (ret != 0) {
		// try force_delete
		ret = combrs->io_ctx->remove(path);
		ret = combrs->io_ctx->remove(path + CEPH_OBJ_SUFFIX) & ret;
		CHECK_RETURN(ret != 0, false);
	}
	// update file meta
	if (update) {
		auto key = std::make_pair(pool, ns);
		auto tm = GetTimeNs() << 1;
		boost::interprocess::message_queue mq(boost::interprocess::open_or_create, GetJdfsUsername().data(),
		                                      CEPH_INDEX_MQ_SIZE, sizeof(Elem));
		if (mq.get_num_msg() > MQ_SIZE_THRESHOLD) {
			PersistChangeInMessageQueueToCeph(&mq);
		}
		auto elem = Conv2Elem(path, pool, ns, tm);
		mq.send(&elem, sizeof(elem), 0);
		{
			std::unique_lock<std::mutex> lk(mtx);
			increment_file_meta[key][path] = tm;
		}
		auto &&hash_key = std::forward_as_tuple(path, pool, ns);
		if (enable_cache) {
			cache.remove(hash_key);
		}
	}
	return true;
}

std::vector<std::string> CephConnector::ListFiles(const std::string &path, const std::string &pool,
                                                  const std::string &ns) {
	std::unordered_map<std::string, int64_t> ret;
	auto key = std::make_pair(pool, ns);
	std::string objname;
	bool raw_empty;

	{
		std::unique_lock<std::mutex> lk(mtx);
		auto prefix_range = increment_file_meta[key].equal_prefix_range(path);
		for (auto it = prefix_range.first; it != prefix_range.second; it++) {
			it.key(objname);
			ret[objname] = it.value();
		}
		raw_empty = raw_file_meta[key].empty();
	}

	{
		if (raw_empty) {
			auto local_cache = GetLocalCache(pool, ns, false);
			if (!Exist(CEPH_INDEX_FILE, pool, ns) && !fs::exists(local_cache)) {
				RefreshFileMeta(pool, ns);
			} else {
				auto process = [&](const string &buf) {
					tsl::deserializer dserial(buf.c_str());
					auto tmp = tsl::htrie_map<char, std::uint64_t>::deserialize(dserial);
					std::string key_buffer;
					auto &&target = raw_file_meta[key];
					std::unique_lock<std::mutex> lk(mtx);
					for (auto it = tmp.begin(); it != tmp.end(); ++it) {
						it.key(key_buffer);
						auto mit = target.find(key_buffer);
						if (mit == target.end() || mit.value() < it.value()) {
							target[key_buffer] = it.value();
						}
					}
				};
				// read from common meta first
				if (Exist(CEPH_INDEX_FILE, pool, ns)) {
					auto sz = Size(CEPH_INDEX_FILE, pool, ns);
					if (sz > 0) {
						std::string buf;
						buf.resize(sz);
						Read(CEPH_INDEX_FILE, pool, ns, 0, buf.data(), sz);
						process(buf);
					}
				}
				// try to read from local
				if (fs::exists(local_cache)) {
					std::ifstream fin(local_cache);
					std::stringstream buf;
					buf << fin.rdbuf();
					auto &&sbuf = buf.str();
					if (!sbuf.empty()) {
						process(sbuf);
					}
				}
			}
		}
	}

	{
		std::unique_lock<std::mutex> lk(mtx);
		auto prefix_range = raw_file_meta[key].equal_prefix_range(path);
		for (auto it = prefix_range.first; it != prefix_range.second; it++) {
			it.key(objname);
			auto eit = ret.find(objname);
			if (eit == ret.end() || eit->second < it.value()) {
				ret[objname] = it.value();
			}
		}
	}
	std::vector<std::string> obj_list;
	for (auto &[k, v] : ret) {
		if (v & 1) {
			obj_list.push_back(k);
		}
	}
	return obj_list;
}

void CephConnector::RefreshFileMeta(const std::string &pool, const std::string &ns) {
	auto combrs = std::make_shared<CombStriper>();
	auto err = GetCombStriper(pool, ns, &combrs);
	CHECK_RETURN(err < 0, void());

	std::vector<std::string> ret;
	std::vector<time_t> query_times;
	for (auto it = combrs->io_ctx->nobjects_begin(); it != combrs->io_ctx->nobjects_end(); it++) {
		if (it->get_nspace() != ns) {
			continue;
		}
		auto &&oid = it->get_oid();
		const auto pos = oid.size() - CEPH_OBJ_SUFFIX.size();
		if (oid.find(CEPH_OBJ_SUFFIX, pos) != std::string::npos) {
			ret.emplace_back(oid.substr(0, pos));
			query_times.push_back(GetTimeNs());
		}
	}
	// update raw_meta
	{
		auto key = std::make_pair(pool, ns);
		std::string buf;
		{
			std::unique_lock<std::mutex> lk(mtx);
			raw_file_meta[key].clear();
			for (size_t i = 0; i < ret.size(); i++) {
				auto tm = (query_times[i] << 1) | 1;
				raw_file_meta[key][ret[i]] = tm;
			}

			tsl::serializer serial(buf);
			raw_file_meta[key].shrink_to_fit();
			raw_file_meta[key].serialize(serial);
		}
		auto ret = Write(CEPH_INDEX_FILE, pool, ns, buf.data(), buf.size(), false);
		if (ret < 0) {
			auto local_cache = GetLocalCache(pool, ns, true);
			if (!local_cache.empty()) {
				std::ofstream fout(local_cache);
				fout << buf;
			}
		}
	}
}

void CephConnector::PersistChangeInMessageQueueToCeph(boost::interprocess::message_queue *mq_ptr) {
	auto &&mq = *mq_ptr;
	if (mq.get_num_msg() == 0) {
		return;
	}
	Elem elem;
	size_t recv_size;
	unsigned int pq;
	std::map<std::pair<std::string, std::string>, tsl::htrie_map<char, std::uint64_t>> mp;
	while (mq.try_receive(&elem, sizeof(elem), recv_size, pq)) {
		auto url = std::string_view(elem.path.data(), elem.sz);
		std::string path, pool, ns;
		ParseUrl(url, pool, ns, path);
		if (path == CEPH_INDEX_FILE) {
			continue;
		}
		auto key = std::make_pair(pool, ns);
		auto it = mp[key].find(path);
		if (it == mp[key].end() || it.value() < elem.tm) {
			mp[key][path] = elem.tm;
		}
	}
	boost::interprocess::message_queue::remove(GetJdfsUsername().data());
	for (auto &[key, v] : mp) {
		auto &&[pool, ns] = key;
		tsl::htrie_map<char, std::uint64_t> map_deserialized;
		if (Exist(CEPH_INDEX_FILE, pool, ns)) {
			auto sz = Size(CEPH_INDEX_FILE, pool, ns);
			// if size of the index file more than 1GB, then it's considered as broken.
			if (sz >= (1 << 30)) {
				Delete(CEPH_INDEX_FILE, pool, ns, false);
				continue;
			}
			if (sz > 0) {
				std::string buf;
				buf.resize(sz);
				Read(CEPH_INDEX_FILE, pool, ns, 0, buf.data(), sz);
				tsl::deserializer dserial(buf.c_str());
				try {
					map_deserialized = tsl::htrie_map<char, std::uint64_t>::deserialize(dserial);
				} catch (...) {
					// index file broken, then remove directly.
					Delete(CEPH_INDEX_FILE, pool, ns, false);
					continue;
				}
			}
		}
		std::string key_buffer;
		for (auto it = v.begin(); it != v.end(); ++it) {
			it.key(key_buffer);
			auto mit = map_deserialized.find(key_buffer);
			if (mit == map_deserialized.end() || mit.value() < it.value()) {
				map_deserialized[key_buffer] = it.value();
			}
		}
		// call this funtion to avoid file size expansion.
		std::string buf;
		tsl::serializer serial(buf);
		map_deserialized.shrink_to_fit();
		map_deserialized.serialize(serial);
		Write(CEPH_INDEX_FILE, pool, ns, const_cast<char *>(buf.c_str()), buf.size(), false);
	}
}

int CephConnector::GetCombStriper(const std::string &pool, const std::string &ns,
                                  std::shared_ptr<CephConnector::CombStriper> *cs) {
	auto io_ctx = std::shared_ptr<librados::IoCtx>(new librados::IoCtx(), [](auto &&io_ctx) {
		if (io_ctx->is_valid()) {
			io_ctx->close();
		}
		delete io_ctx;
	});
	auto ret = cluster.ioctx_create(pool.c_str(), *io_ctx);
	CHECK_RETURN(ret < 0, ret);
	uint64_t alignment;
	ret = io_ctx->pool_required_alignment2(&alignment);
	CHECK_RETURN(ret < 0, ret);
	io_ctx->set_namespace(ns);
	(*cs)->rs = std::make_shared<libradosstriper::RadosStriper>();
	ret = libradosstriper::RadosStriper::striper_create(*io_ctx, (*cs)->rs.get());
	CHECK_RETURN(ret < 0, ret);
	(*cs)->io_ctx = std::move(io_ctx);
	return 0;
}
} // namespace duckdb