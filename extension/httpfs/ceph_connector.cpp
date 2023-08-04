#include "ceph_connector.hpp"

#include "radosstriper/libradosstriper.hpp"

#include <cstring>
#include <iostream>

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

static constexpr const char *CLUSTER_NAME = "ceph";
pid_t CephConnector::pid_ = -1;
void CephConnector::init() {
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
	cluster = std::make_unique<librados::Rados>();
	int ret = cluster->init2(username.c_str(), CLUSTER_NAME, 0);
	if (ret < 0) {
		throw std::runtime_error(std::string("Couldn't init cluster ") + strerror(-ret));
	}
	ret = cluster->conf_read_file(config_path.c_str());
	if (ret < 0) {
		throw std::runtime_error(std::string("Couldn't read conf file ") + strerror(-ret));
	}
	ret = cluster->connect();
	if (ret < 0) {
		throw std::runtime_error(std::string("Couldn't connect to cluster ") + strerror(-ret));
	}
}

[[nodiscard]] size_t CephConnector::BKDRHash(const std::string &path, const std::string &pool, const std::string &ns) {
	size_t ret = 0;
	auto hash = [&ret](const std::string &s) {
		for (auto &ch : s) {
			ret = ret * 131 + ch;
		}
	};
	hash(path);
	hash(pool);
	hash(ns);
	return ret;
}

[[nodiscard]] int64_t CephConnector::Size(const std::string &path, const std::string &pool, const std::string &ns) {
	auto key = BKDRHash(path, pool, ns);
	// -1 not exist or >= 0 return directly
	if (int64_t ret; meta_cache.tryGet(key, ret) && ret >= -1) {
		return ret;
	}
	auto combrs = this->getCombStriper(pool, ns);
	CHECK_RETRUN(!combrs, -1);
	uint64_t size = 0;
	combrs->rs->stat2(path, &size, nullptr);
	meta_cache.insert(key, size);
	return size;
}

[[nodiscard]] bool CephConnector::Exist(const std::string &path, const std::string &pool, const std::string &ns) {
	auto key = BKDRHash(path, pool, ns);
	// -1 indicate not exist
	// -4396 indicate exist, but size is unkown
	static constexpr int64_t EXIST_FLAG = -4396;
	if (int64_t ret; meta_cache.tryGet(key, ret) && ret != -1) {
		return true;
	}
	auto combrs = this->getCombStriper(pool, ns);
	CHECK_RETRUN(!combrs, false);
	ceph::bufferlist bufferlist;
	auto ret = combrs->io_ctx->getxattr(path + ".0000000000000000", "striper.layout.object_size", bufferlist);
	if (ret >= 0) {
		meta_cache.insert(key, EXIST_FLAG);
	} else {
		meta_cache.insert(key, -1);
	}
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

[[nodiscard]] int64_t CephConnector::Read(const std::string &path, const std::string &pool, const std::string &ns,
                                          int64_t file_offset, char *buffer_out, int64_t buffer_out_len) {
	auto key = BKDRHash(path, pool, ns);
	// if hit cache, then read from cache.
	if (std::shared_ptr<std::vector<char>> buf; small_files_cache.tryGet(key, buf)) {
		memcpy(buffer_out, buf->data() + file_offset, buffer_out_len);
		return buffer_out_len;
	}

	// cache small files, usually they are some meta files
	auto sz = Size(path, pool, ns);
	if (sz <= SMALL_FILE_THRESHOLD) {
		auto buf = std::make_shared<std::vector<char>>(sz);
		doRead(path, pool, ns, 0, buf->data(), sz);
		memcpy(buffer_out, buf->data() + file_offset, buffer_out_len);
		small_files_cache.insert(key, buf);
		return buffer_out_len;
	}
	// read directly
	return doRead(path, pool, ns, file_offset, buffer_out, buffer_out_len);
}

[[nodiscard]] int64_t CephConnector::Write(const std::string &path, const std::string &pool, const std::string &ns,
                                           int64_t file_offset, char *buffer_in, int64_t buffer_in_len) {
	CHECK_RETRUN(Exist(path, pool, ns) && !Delete(path, pool, ns), -1);
	auto combrs = getCombStriper(pool, ns);
	CHECK_RETRUN(!combrs, -1);
	std::vector<std::unique_ptr<librados::AioCompletion>> completions;
	int64_t transmited = 0;
	for (; transmited < buffer_in_len; transmited += SPLIT_SIZE) // create write ops
	{
		const auto transfer_size = std::min(SPLIT_SIZE, buffer_in_len - transmited);
		auto bl = ceph::bufferlist::static_from_mem(const_cast<char *>(buffer_in) + transmited, transfer_size);
		completions.emplace_back(librados::Rados::aio_create_completion());
		auto ret = combrs->rs->aio_write(path, completions.back().get(), bl, transfer_size, transmited + file_offset);
		CHECK_RETRUN(ret < 0, ret);
	}
	// wait for all job complete
	for (auto &complete : completions) {
		int ret = 0;
		if (!complete->is_complete()) {
			ret = complete->wait_for_complete();
		}
		CHECK_RETRUN(ret != 0, ret);
		ret = complete->get_return_value();
		CHECK_RETRUN(ret != 0, ret);
	}

	{
		// set obj size, to make compatiable with jdfs;
		ceph::bufferlist bl;
		bl.append("0.1");
		int ret = combrs->rs->setxattr(path, "_version", bl);
		CHECK_RETRUN(ret != 0, ret);
		bl.clear();
		bl.append(std::to_string(buffer_in_len));
		ret = combrs->rs->setxattr(path, "_size", bl);
		CHECK_RETRUN(ret != 0, ret);
	}

	return transmited;
}

[[nodiscard]] bool CephConnector::Delete(const std::string &path, const std::string &pool, const std::string &ns) {
	auto key = BKDRHash(path, pool, ns);
	// remove from cache first.
	meta_cache.remove(key);
	small_files_cache.remove(key);
	auto combrs = getCombStriper(pool, ns);
	CHECK_RETRUN(!combrs, false);
	combrs->rs->rmxattr(path, "_version");
	combrs->rs->rmxattr(path, "_size");
	auto ret = combrs->rs->remove(path);
	if (ret != 0) {
		// try force_delete
		ret = combrs->io_ctx->remove(path);
		ret = combrs->io_ctx->remove(path + ".0000000000000000") & ret;
		CHECK_RETRUN(ret != 0, false);
	}
	return true;
}

std::shared_ptr<CephConnector::CombStriper> CephConnector::getCombStriper(const std::string &pool,
                                                                          const std::string &ns) {
	auto io_ctx = std::shared_ptr<librados::IoCtx>(new librados::IoCtx(), [](auto &&io_ctx) {
		if (io_ctx->is_valid()) {
			io_ctx->close();
		}
		delete io_ctx;
	});
	CHECK_RETRUN(cluster->ioctx_create(pool.c_str(), *io_ctx) < 0, {});
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