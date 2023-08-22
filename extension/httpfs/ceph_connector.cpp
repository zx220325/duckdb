#include "ceph_connector.hpp"

#include "radosstriper/libradosstriper.hpp"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <chrono>
#include <cstring>
#include <ctime>
#include <experimental/filesystem>
#include <fstream>
#include <iostream>
#include <pwd.h>
#include <random>
#include <sstream>
#include <string>
#include <sys/types.h>
#include <unistd.h>

namespace duckdb {

#define CHECK_RETRUN(expr, value)                                                                                      \
	do {                                                                                                               \
		if (expr) {                                                                                                    \
			return value;                                                                                              \
		}                                                                                                              \
	} while (0)

static constexpr int64_t SPLIT_SIZE = 1024 * 1024 * 1024;

static std::string getEnv(const std::string &ENV) {
	auto ptr = std::getenv(ENV.c_str());
	std::string ret;
	if (ptr) {
		ret = std::string(ptr);
	}
	return ret;
}

static const std::string LOCK_NAME = "default_lock";
Lock::Lock(const std::string &path, const std::string &pool, const std::string &ns,
           const std::shared_ptr<librados::IoCtx> &io_ctx)
    : path_(path), io_ctx_(io_ctx) {
	auto ret = io_ctx->lock_exclusive(path, LOCK_NAME, "", "", nullptr, 0);
}

Lock::~Lock() {
	auto ret = io_ctx_->unlock(path_, LOCK_NAME, "");
}

static constexpr const char *CLUSTER_NAME = "ceph";
pid_t CephConnector::pid_ = -1;

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

static duckdb::Elem Conv2Elem(const std::string &path, const std::string &pool, const std::string &ns,
                              std::uint64_t tm) {
	auto s = "ceph://" + pool + "//" + ns + "//" + path;
	duckdb::Elem elem;
	if (s.size() + 16 > sizeof(Elem)) {
		throw std::runtime_error("path is to long");
	}
	elem.sz = s.size();
	memcpy(elem.path.data(), s.data(), s.size());
	elem.tm = tm;
	return elem;
}

static std::uint64_t get_ns_time() {
	auto tm = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::nanoseconds>(tm.time_since_epoch()).count();
}

static const std::string CEPH_OBJ_SUFFIX = ".0000000000000000";

CephConnector &CephConnector::connnector_singleton() {
	static CephConnector instance;
	auto cur_pid = getpid();
	if (cur_pid != pid_) {
		instance.initialize();
		pid_ = cur_pid;
	}
	return instance;
}

void CephConnector::initialize() {
	std::string username = getEnv("SYS_JDFS_USERNAME");
	if (username.empty()) {
		username = getEnv("JDFS_USERNAME");
	}
	if (username.empty()) {
		throw std::runtime_error("Environment Variable JDFS_USERNAME was not found!");
	}
	std::string config_path = getEnv("SYS_JDFS_CONFIG_PATH");
	if (config_path.empty()) {
		config_path = getEnv("JDFS_CONFIG_PATH");
	}
	if (config_path.empty()) {
		throw std::runtime_error("Environment Variable JDFS_CONFIG_PATH was not found!");
	}

	int ret = cluster.init2(username.c_str(), CLUSTER_NAME, 0);
	if (ret < 0) {
		throw std::runtime_error(std::string("Couldn't init cluster ") + strerror(-ret));
	}
	ret = cluster.conf_read_file(config_path.c_str());
	if (ret < 0) {
		throw std::runtime_error(std::string("Couldn't read conf file ") + strerror(-ret));
	}
	ret = cluster.connect();
	if (ret < 0) {
		throw std::runtime_error(std::string("Couldn't connect to cluster ") + strerror(-ret));
	}
}

int64_t CephConnector::Size(const std::string &path, const std::string &pool, const std::string &ns, time_t *mtm) {
	auto combrs = this->getCombStriper(pool, ns);
	CHECK_RETRUN(!combrs, -1);
	uint64_t size = 0;
	timespec tsp {};
	combrs->rs->stat2(path, &size, &tsp);
	if (mtm) {
		*mtm = tsp.tv_sec;
	}
	return size;
}

bool CephConnector::Exist(const std::string &path, const std::string &pool, const std::string &ns) {
	auto combrs = this->getCombStriper(pool, ns);
	CHECK_RETRUN(!combrs, false);
	ceph::bufferlist bufferlist;
	auto ret = combrs->io_ctx->getxattr(path + CEPH_OBJ_SUFFIX, "striper.layout.object_size", bufferlist);
	return ret >= 0;
}

int64_t CephConnector::doRead(const std::string &path, const std::string &pool, const std::string &ns,
                              int64_t file_offset, char *buffer_out, int64_t buffer_out_len) {
	auto combrs = getCombStriper(pool, ns);
	CHECK_RETRUN(!combrs, -1);
	int64_t has_read = 0;
	for (int64_t offset = 0; offset < buffer_out_len; offset += SPLIT_SIZE) {
		ceph::bufferlist bl;
		auto ret = combrs->rs->read(path, &bl, std::min(SPLIT_SIZE, buffer_out_len - offset), offset + file_offset);
		CHECK_RETRUN(ret <= 0, -1);
		for (auto &buf : bl.buffers()) {
			memcpy(buffer_out + has_read, buf.c_str(), buf.length());
			has_read += buf.length();
		}
	}
	return has_read;
}

int64_t CephConnector::Read(const std::string &path, const std::string &pool, const std::string &ns,
                            int64_t file_offset, char *buffer_out, int64_t buffer_out_len) {
	auto sz = Size(path, pool, ns);
	return doRead(path, pool, ns, file_offset, buffer_out, buffer_out_len);
}

int64_t CephConnector::Write(const std::string &path, const std::string &pool, const std::string &ns, char *buffer_in,
                             int64_t buffer_in_len, bool update) {
	CHECK_RETRUN(Exist(path, pool, ns) && !Delete(path, pool, ns, false), -1);
	auto combrs = getCombStriper(pool, ns);
	CHECK_RETRUN(!combrs, -1);
	std::vector<std::unique_ptr<librados::AioCompletion>> completions;
	int64_t transmited = 0;
	for (; transmited < buffer_in_len; transmited += SPLIT_SIZE) // create write ops
	{
		const auto transfer_size = std::min(SPLIT_SIZE, buffer_in_len - transmited);
		auto bl = ceph::bufferlist::static_from_mem(const_cast<char *>(buffer_in) + transmited, transfer_size);
		completions.emplace_back(librados::Rados::aio_create_completion());
		auto ret = combrs->rs->aio_write(path, completions.back().get(), bl, transfer_size, transmited);
		CHECK_RETRUN(ret < 0, ret);
	}
	// wait for all job complete
	for (auto &complete : completions) {
		int ret = 0;
		if (!complete->is_complete()) {
			ret = complete->wait_for_complete();
		}
		CHECK_RETRUN(ret != 0, -1);
		ret = complete->get_return_value();
		CHECK_RETRUN(ret != 0, -1);
	}
	{
		// set obj size, to make compatiable with jdfs;
		ceph::bufferlist bl;
		bl.append("0.1");
		int ret = combrs->rs->setxattr(path, "_version", bl);
		CHECK_RETRUN(ret != 0, -1);
		bl.clear();
		bl.append(std::to_string(buffer_in_len));
		ret = combrs->rs->setxattr(path, "_size", bl);
		CHECK_RETRUN(ret != 0, -1);
	}

	// update file meta
	if (update) {
		// std::unique_lock<std::mutex> lk(mtx_);
		auto key = std::make_pair(pool, ns);
		auto tm = (get_ns_time() << 1) | 1;
		boost::interprocess::message_queue mq(boost::interprocess::open_or_create, duckdb::CEPH_INDEX_MQ_NAME.c_str(),
		                                      duckdb::CEPH_INDEX_MQ_SIZE, sizeof(duckdb::Elem));
		auto elem = Conv2Elem(path, pool, ns, tm);
		mq.send(&elem, sizeof(elem), 0);
		increment_file_meta[key][path] = tm;
	}
	return buffer_in_len;
}

bool CephConnector::Delete(const std::string &path, const std::string &pool, const std::string &ns, bool update) {
	auto combrs = getCombStriper(pool, ns);
	CHECK_RETRUN(!combrs, false);
	combrs->rs->rmxattr(path, "_version");
	combrs->rs->rmxattr(path, "_size");
	auto ret = combrs->rs->remove(path);
	if (ret != 0) {
		// try force_delete
		ret = combrs->io_ctx->remove(path);
		ret = combrs->io_ctx->remove(path + CEPH_OBJ_SUFFIX) & ret;
		CHECK_RETRUN(ret != 0, false);
	}
	// update file meta
	if (update) {
		auto key = std::make_pair(pool, ns);
		auto tm = get_ns_time() << 1;
		boost::interprocess::message_queue mq(boost::interprocess::open_or_create, duckdb::CEPH_INDEX_MQ_NAME.c_str(),
		                                      duckdb::CEPH_INDEX_MQ_SIZE, sizeof(duckdb::Elem));
		auto elem = Conv2Elem(path, pool, ns, tm);
		mq.send(&elem, sizeof(elem), 0);
		increment_file_meta[key][path] = tm;
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
		// std::unique_lock<std::mutex> lk(mtx_);
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
				auto &&target = raw_file_meta[key];
				auto process = [&](const string &buf) {
					tsl::deserializer dserial(buf.c_str());
					auto tmp = tsl::htrie_map<char, std::uint64_t>::deserialize(dserial);
					std::string key_buffer;
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
					if (sbuf.size() > 0) {
						process(sbuf);
					}
				}
			}
		}
	}

	{
		// std::unique_lock<std::mutex> lk(mtx_);
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
	auto combrs = getCombStriper(pool, ns);
	CHECK_RETRUN(!combrs, void());

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
			query_times.push_back(get_ns_time());
		}
	}
	// update raw_meta
	{
		auto key = std::make_pair(pool, ns);
		raw_file_meta[key].clear();
		for (size_t i = 0; i < ret.size(); i++) {
			auto tm = (query_times[i] << 1) | 1;
			raw_file_meta[key][ret[i]] = tm;
		}
		std::string buf;
		tsl::serializer serial(buf);
		raw_file_meta[key].serialize(serial);
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

std::shared_ptr<CephConnector::CombStriper> CephConnector::getCombStriper(const std::string &pool,
                                                                          const std::string &ns) {
	auto io_ctx = std::shared_ptr<librados::IoCtx>(new librados::IoCtx(), [](auto &&io_ctx) {
		if (io_ctx->is_valid()) {
			io_ctx->close();
		}
		delete io_ctx;
	});
	CHECK_RETRUN(cluster.ioctx_create(pool.c_str(), *io_ctx) < 0, {});
	uint64_t alignment;
	CHECK_RETRUN(io_ctx->pool_required_alignment2(&alignment) < 0, {});
	io_ctx->set_namespace(ns);
	auto combStriper = std::make_shared<CombStriper>();
	combStriper->rs = std::make_shared<libradosstriper::RadosStriper>();
	CHECK_RETRUN(libradosstriper::RadosStriper::striper_create(*io_ctx, combStriper->rs.get()) < 0, {});
	combStriper->io_ctx = std::move(io_ctx);
	return combStriper;
}
} // namespace duckdb