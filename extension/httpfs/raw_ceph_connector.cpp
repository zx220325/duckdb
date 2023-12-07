#include "raw_ceph_connector.hpp"

#include "duckdb/common/assert.hpp"
#include "rados/librados.hpp"
#include "radosstriper/libradosstriper.hpp"
#include "utils.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <system_error>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

namespace duckdb {

namespace {

class RadosErrorCategory : public std::error_category {
public:
	static const RadosErrorCategory &GetSingleton() noexcept {
		static RadosErrorCategory INSTANCE;
		return INSTANCE;
	}

	static std::error_code GetErrorCode(int err_code) noexcept {
		return std::error_code {err_code, GetSingleton()};
	}

	const char *name() const noexcept override {
		return "rados";
	}

	std::string message(int err_code) const override {
		return std::strerror(err_code);
	}
};

constexpr const char *CEPH_CLUSTER_NAME = "ceph";
constexpr std::size_t IO_SPLIT_SIZE = 1024 * 1024 * 1024;

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

std::size_t CombineHash(std::size_t h1, std::size_t h2) noexcept {
	return h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2);
}

template <std::size_t Idx, typename... T>
int CompareFieldsImpl(const std::tuple<const T &...> &first, const std::tuple<const T &...> &second) noexcept {
	if constexpr (Idx >= sizeof...(T)) {
		return 0;
	} else if (std::get<Idx>(first) < std::get<Idx>(second)) {
		return -1;
	} else if (std::get<Idx>(second) < std::get<Idx>(first)) {
		return 1;
	} else {
		return CompareFieldsImpl<Idx + 1, T...>(first, second);
	}
}

template <typename... T>
int CompareFields(std::tuple<const T &...> first, std::tuple<const T &...> second) noexcept {
	return CompareFieldsImpl<0, T...>(first, second);
}

void DeleteRadosIoContext(librados::IoCtx *io_ctx) noexcept {
	if (io_ctx->is_valid()) {
		io_ctx->close();
	}
	delete io_ctx;
}

} // namespace

std::size_t CephNamespace::GetHashCode() const noexcept {
	auto pool_hash = std::hash<std::string> {}(pool);
	auto ns_hash = std::hash<std::string> {}(ns);
	return CombineHash(pool_hash, ns_hash);
}

bool operator==(const CephNamespace &lhs, const CephNamespace &rhs) noexcept {
	return lhs.pool == rhs.pool && lhs.ns == rhs.ns;
}

bool operator!=(const CephNamespace &lhs, const CephNamespace &rhs) noexcept {
	return !(lhs == rhs);
}

bool operator<(const CephNamespace &lhs, const CephNamespace &rhs) noexcept {
	const auto &[lhs_pool, lhs_ns] = lhs;
	const auto &[rhs_pool, rhs_ns] = rhs;
	return CompareFields(std::tie(lhs_pool, lhs_ns), std::tie(rhs_pool, rhs_ns)) < 0;
}

bool operator<=(const CephNamespace &lhs, const CephNamespace &rhs) noexcept {
	const auto &[lhs_pool, lhs_ns] = lhs;
	const auto &[rhs_pool, rhs_ns] = rhs;
	return CompareFields(std::tie(lhs_pool, lhs_ns), std::tie(rhs_pool, rhs_ns)) <= 0;
}

bool operator>=(const CephNamespace &lhs, const CephNamespace &rhs) noexcept {
	const auto &[lhs_pool, lhs_ns] = lhs;
	const auto &[rhs_pool, rhs_ns] = rhs;
	return CompareFields(std::tie(lhs_pool, lhs_ns), std::tie(rhs_pool, rhs_ns)) >= 0;
}

bool operator>(const CephNamespace &lhs, const CephNamespace &rhs) noexcept {
	const auto &[lhs_pool, lhs_ns] = lhs;
	const auto &[rhs_pool, rhs_ns] = rhs;
	return CompareFields(std::tie(lhs_pool, lhs_ns), std::tie(rhs_pool, rhs_ns)) > 0;
}

std::size_t CephPath::GetHashCode() const noexcept {
	auto ns_hash = ns.GetHashCode();
	auto path_hash = std::hash<std::string> {}(path);
	return CombineHash(ns_hash, path_hash);
}

std::string CephPath::ToString() const noexcept {
	std::ostringstream builder;
	builder << "ceph://" << ns.pool << "//" << ns.ns << "//" << path;
	return builder.str();
}

bool operator==(const CephPath &lhs, const CephPath &rhs) noexcept {
	return lhs.ns == rhs.ns && lhs.path == rhs.path;
}

bool operator!=(const CephPath &lhs, const CephPath &rhs) noexcept {
	return !(lhs == rhs);
}

bool operator<(const CephPath &lhs, const CephPath &rhs) noexcept {
	const auto &[lhs_ns, lhs_path] = lhs;
	const auto &[rhs_ns, rhs_path] = rhs;
	return CompareFields(std::tie(lhs_ns, lhs_path), std::tie(rhs_ns, rhs_path)) < 0;
}

bool operator<=(const CephPath &lhs, const CephPath &rhs) noexcept {
	const auto &[lhs_ns, lhs_path] = lhs;
	const auto &[rhs_ns, rhs_path] = rhs;
	return CompareFields(std::tie(lhs_ns, lhs_path), std::tie(rhs_ns, rhs_path)) <= 0;
}

bool operator>=(const CephPath &lhs, const CephPath &rhs) noexcept {
	const auto &[lhs_ns, lhs_path] = lhs;
	const auto &[rhs_ns, rhs_path] = rhs;
	return CompareFields(std::tie(lhs_ns, lhs_path), std::tie(rhs_ns, rhs_path)) >= 0;
}

bool operator>(const CephPath &lhs, const CephPath &rhs) noexcept {
	const auto &[lhs_ns, lhs_path] = lhs;
	const auto &[rhs_ns, rhs_path] = rhs;
	return CompareFields(std::tie(lhs_ns, lhs_path), std::tie(rhs_ns, rhs_path)) > 0;
}

struct RawCephConnector::RadosContext {
	static std::unique_ptr<RadosContext> Create(librados::Rados &rados, const CephNamespace &ns,
	                                            std::error_code &ec) noexcept {
		auto ctx = CreateWithoutStriper(rados, ns, ec);
		if (ec) {
			return nullptr;
		}

		// Create and initialize RadosStriper.
		auto striper = std::make_unique<libradosstriper::RadosStriper>();
		if (auto ret = libradosstriper::RadosStriper::striper_create(*ctx->io_ctx, striper.get()); ret < 0) {
			ec = RadosErrorCategory::GetErrorCode(-ret);
			return nullptr;
		}

		ec = std::error_code {};
		ctx->striper = std::move(striper);
		return ctx;
	}

	static std::unique_ptr<RadosContext> CreateWithoutStriper(librados::Rados &rados, const CephNamespace &ns,
	                                                          std::error_code &ec) noexcept {
		std::unique_ptr<librados::IoCtx, void (*)(librados::IoCtx *) noexcept> io_ctx {new librados::IoCtx {},
		                                                                               &DeleteRadosIoContext};
		if (auto ret = rados.ioctx_create(ns.pool.c_str(), *io_ctx); ret < 0) {
			ec = RadosErrorCategory::GetErrorCode(-ret);
			return nullptr;
		}

		std::uint64_t alignment;
		if (auto ret = io_ctx->pool_required_alignment2(&alignment); ret < 0) {
			ec = RadosErrorCategory::GetErrorCode(-ret);
			return nullptr;
		}

		io_ctx->set_namespace(ns.ns);

		ec = std::error_code {};
		return std::unique_ptr<RadosContext> {new RadosContext {std::move(io_ctx), nullptr}};
	}

	std::unique_ptr<librados::IoCtx, void (*)(librados::IoCtx *) noexcept> io_ctx;
	std::unique_ptr<libradosstriper::RadosStriper> striper;
};

RawCephConnector::RawCephConnector() : cluster {} {
	Connect();
}

void RawCephConnector::Connect() {
	if (GetJdfsUsername().empty()) {
		throw std::runtime_error("can not find JDFS_USERNAME in environment variable");
	}

	auto ceph_args = GetEnv("CEPH_ARGS");
	if (!ceph_args.empty()) {
		if (auto err = cluster.init2(GetJdfsUsername().data(), nullptr, 0); err < 0) {
			throw std::runtime_error(std::string("Couldn't init cluster ") + std::strerror(-err));
		}
		if (auto err = cluster.conf_parse_env("CEPH_ARGS"); err < 0) {
			throw std::runtime_error(std::string("Couldn't parse config ") + std::strerror(-err));
		}
	} else {
		std::string config_path = GetEnv("SYS_JDFS_CONFIG_PATH");
		if (config_path.empty()) {
			config_path = GetEnv("JDFS_CONFIG_PATH");
		}
		if (config_path.empty()) {
			throw std::runtime_error("Environment Variable JDFS_CONFIG_PATH was not found!");
		}

		if (auto err = cluster.init2(GetJdfsUsername().data(), CEPH_CLUSTER_NAME, 0); err < 0) {
			throw std::runtime_error(std::string("Couldn't init cluster ") + std::strerror(-err));
		}

		if (auto err = cluster.conf_read_file(config_path.c_str()); err < 0) {
			throw std::runtime_error(std::string("Couldn't read conf file ") + std::strerror(-err));
		}
	}

	if (auto err = cluster.connect(); err < 0) {
		throw std::runtime_error(std::string("Couldn't connect to cluster ") + std::strerror(-err));
	}
}

CephStat RawCephConnector::Stat(const CephPath &path, std::error_code &ec) noexcept {
	auto ctx = RadosContext::Create(cluster, path.ns, ec);
	if (ec) {
		return {};
	}

	std::uint64_t raw_size {0};
	::time_t raw_tm {0};
	if (auto err = ctx->striper->stat(path.path, &raw_size, &raw_tm); err < 0 && err != -ENODATA) {
		// Stat an empty file will return -ENODATA.
		ec = RadosErrorCategory::GetErrorCode(-err);
		return {};
	}
	if (raw_size == 0) {
		ceph::bufferlist bufferlist;
		if (auto err = ctx->io_ctx->getxattr(path.path + CEPH_OBJ_SUFFIX, "striper.layout.object_size", bufferlist);
		    err < 0 && err != -ENODATA) {
			// Stat an empty file will return -ENODATA.
			ec = RadosErrorCategory::GetErrorCode(-err);
			return {};
		}
	}

	std::size_t size = raw_size;
	typename std::chrono::system_clock::time_point tm {std::chrono::seconds {raw_tm}};

	ec = std::error_code {};
	return {size, tm};
}

bool RawCephConnector::Exist(const CephPath &path, std::error_code &ec) noexcept {
	auto stat = Stat(path, ec);
	if (ec) {
		if (ec.value() == ENOENT) {
			ec = std::error_code {};
		}
		return false;
	}

	return stat.size > 0;
}

bool RawCephConnector::RadosExist(const CephPath &path, std::error_code &ec) noexcept {
	auto ctx = RadosContext::CreateWithoutStriper(cluster, path.ns, ec);
	if (ec) {
		return false;
	}

	ec = std::error_code {};

	std::uint64_t raw_size;
	::time_t raw_tm;
	if (auto err = ctx->io_ctx->stat(path.path, &raw_size, &raw_tm); err < 0) {
		if (err != -ENOENT) {
			ec = RadosErrorCategory::GetErrorCode(-err);
		}
		return false;
	}

	return true;
}

void RawCephConnector::RadosCreate(const CephPath &path, std::error_code &ec) noexcept {
	auto ctx = RadosContext::CreateWithoutStriper(cluster, path.ns, ec);
	if (ec) {
		return;
	}

	ec = std::error_code {};
	if (auto err = ctx->io_ctx->create(path.path, false); err < 0) {
		ec = RadosErrorCategory::GetErrorCode(-err);
	}
}

std::size_t RawCephConnector::Read(const CephPath &path, std::uint64_t file_offset, void *buffer,
                                   std::size_t buffer_size, std::error_code &ec) noexcept {
	auto ctx = RadosContext::Create(cluster, path.ns, ec);
	if (ec) {
		return 0;
	}

	ec = std::error_code {};

	std::size_t bytes_read = 0;
	while (bytes_read < buffer_size) {
		ceph::bufferlist bl;
		if (auto ret = ctx->striper->read(path.path, &bl, std::min(IO_SPLIT_SIZE, buffer_size - bytes_read),
		                                  file_offset + bytes_read);
		    ret < 0) {
			ec = RadosErrorCategory::GetErrorCode(-ret);
			break;
		}

		std::size_t bytes_read_this_time = 0;
		for (auto &buf : bl.buffers()) {
			std::memcpy(reinterpret_cast<char *>(buffer) + bytes_read + bytes_read_this_time, buf.c_str(),
			            buf.length());
			bytes_read_this_time += buf.length();
		}

		if (bytes_read_this_time == 0) {
			break;
		}

		bytes_read += bytes_read_this_time;
	}

	return bytes_read;
}

std::size_t RawCephConnector::Write(const CephPath &path, const void *buffer, std::size_t buffer_size,
                                    std::error_code &ec) noexcept {
	auto ctx = RadosContext::Create(cluster, path.ns, ec);
	if (ec) {
		return 0;
	}
	int rc = ctx->striper->trunc(path.path, 0);
	if (rc && rc != -ENOENT) {
		ec = RadosErrorCategory::GetErrorCode(-rc);
		return 0;
	}
	std::vector<std::unique_ptr<librados::AioCompletion>> completions;
	completions.reserve((buffer_size + IO_SPLIT_SIZE - 1) / IO_SPLIT_SIZE);

	// Begin all asynchronous write requests.
	for (std::size_t offset = 0; offset < buffer_size; offset += IO_SPLIT_SIZE) {
		auto data_ptr = reinterpret_cast<const char *>(buffer) + offset;
		auto write_size = std::min(IO_SPLIT_SIZE, buffer_size - offset);
		auto bl = ceph::bufferlist::static_from_mem(const_cast<char *>(data_ptr), write_size);
		completions.emplace_back(librados::Rados::aio_create_completion());

		if (auto ret = ctx->striper->aio_write(path.path, completions.back().get(), bl, write_size, offset); ret < 0) {
			ec = RadosErrorCategory::GetErrorCode(-ret);
			return 0;
		}
	}

	// Wait for all asynchronous operations to complete.
	for (auto &io_comp : completions) {
		if (!io_comp->is_complete()) {
			if (auto ret = io_comp->wait_for_complete(); ret < 0) {
				ec = RadosErrorCategory::GetErrorCode(-ret);
				return 0;
			}
		}

		if (auto ret = io_comp->get_return_value(); ret < 0) {
			ec = RadosErrorCategory::GetErrorCode(-ret);
			return 0;
		}
	}

	// Set object's size, to make it compatiable with JDFS.
	ceph::bufferlist bl;
	bl.append("0.1");
	if (auto ret = ctx->striper->setxattr(path.path, "_version", bl); ret < 0) {
		ec = RadosErrorCategory::GetErrorCode(-ret);
		return 0;
	}

	bl.clear();
	bl.append(std::to_string(buffer_size));
	if (auto ret = ctx->striper->setxattr(path.path, "_size", bl); ret < 0) {
		ec = RadosErrorCategory::GetErrorCode(-ret);
		return 0;
	}

	ec = std::error_code {};
	return buffer_size;
}

void RawCephConnector::Delete(const CephPath &path, std::error_code &ec) noexcept {
	auto ctx = RadosContext::Create(cluster, path.ns, ec);
	if (ec) {
		return;
	}

	ctx->striper->rmxattr(path.path, "_version");
	ctx->striper->rmxattr(path.path, "_size");

	if (auto ret = ctx->striper->remove(path.path); ret < 0) {
		// Cannot delete through libradosstriper. Try force delete through librados.
		ret = ctx->io_ctx->remove(path.path + CEPH_OBJ_SUFFIX);
		if (ret < 0) {
			ec = RadosErrorCategory::GetErrorCode(ret);
			return;
		}
	}
	ec = std::error_code {};
}

void RawCephConnector::RadosDelete(const CephPath &path, std::error_code &ec) noexcept {
	auto ctx = RadosContext::CreateWithoutStriper(cluster, path.ns, ec);
	if (ec) {
		return;
	}

	ec = std::error_code {};
	if (auto err = ctx->io_ctx->remove(path.path); err < 0) {
		ec = RadosErrorCategory::GetErrorCode(-err);
	}
}

std::vector<std::string> RawCephConnector::ListFiles(const CephNamespace &ns, std::error_code &ec) noexcept {
	return ListFilesAndFilter(
	    ns, [](const std::string &, std::error_code &) { return true; }, ec);
}

std::vector<std::string>
RawCephConnector::ListFilesAndFilter(const CephNamespace &ns,
                                     const std::function<bool(const std::string &, std::error_code &)> &predicate,
                                     std::error_code &ec) {
	return ListFilesAndTransform(
	    ns,
	    [&predicate](std::string oid, std::error_code &ec) -> std::optional<std::string> {
		    if (!predicate(std::as_const(oid), ec) || ec) {
			    return std::nullopt;
		    }
		    return oid;
	    },
	    ec);
}

std::vector<std::string> RawCephConnector::ListFilesAndTransform(
    const CephNamespace &ns, const std::function<std::optional<std::string>(std::string, std::error_code &)> &transform,
    std::error_code &ec) {
	auto ctx = RadosContext::CreateWithoutStriper(cluster, ns, ec);
	if (ec) {
		return {};
	}

	ec = std::error_code {};

	// use unordered_set to reduce memory usage, for radosstriper, a 400M file will be splited to 100 * 4M.
	std::unordered_set<std::string> names;
	auto range_begin = ctx->io_ctx->nobjects_begin();
	auto range_end = ctx->io_ctx->nobjects_end();
	for (auto it = range_begin; it != range_end; ++it) {
		if (it->get_nspace() != ns.ns) {
			continue;
		}

		auto object_name = it->get_oid();
		auto transformed_object_name = transform(std::move(object_name), ec);
		if (ec) {
			return {names.begin(), names.end()};
		}
		if (transformed_object_name.has_value()) {
			names.emplace(std::move(transformed_object_name.value()));
		}
	}

	return {names.begin(), names.end()};
}

std::map<std::string, librados::bufferlist>
RawCephConnector::GetOmapKv(const CephPath &path, const std::set<std::string> &keys, std::error_code &ec) noexcept {
	auto ctx = RadosContext::CreateWithoutStriper(cluster, path.ns, ec);
	if (ec) {
		return {};
	}

	std::map<std::string, librados::bufferlist> ret;

	if (keys.empty()) {
		if (auto err = ctx->io_ctx->omap_get_vals(path.path, "", std::numeric_limits<std::uint64_t>::max(), &ret);
		    err < 0) {
			return {};
		}
	} else {
		if (auto err = ctx->io_ctx->omap_get_vals_by_keys(path.path, keys, &ret); err < 0) {
			ec = RadosErrorCategory::GetErrorCode(-err);
			return {};
		}
	}

	ec = std::error_code {};
	return ret;
}

void RawCephConnector::SetOmapKv(const CephPath &path, const std::map<std::string, librados::bufferlist> &kv,
                                 std::error_code &ec) noexcept {
	auto ctx = RadosContext::CreateWithoutStriper(cluster, path.ns, ec);
	if (ec) {
		return;
	}

	if (auto err = ctx->io_ctx->omap_set(path.path, kv); err < 0) {
		ec = RadosErrorCategory::GetErrorCode(-err);
		return;
	}

	ec = std::error_code {};
}

void RawCephConnector::DeleteOmapKeys(const CephPath &path, const std::set<std::string> &keys,
                                      std::error_code &ec) noexcept {
	auto ctx = RadosContext::CreateWithoutStriper(cluster, path.ns, ec);
	if (ec) {
		return;
	}

	ec = std::error_code {};
	if (auto err = ctx->io_ctx->omap_rm_keys(path.path, keys); err < 0) {
		ec = RadosErrorCategory::GetErrorCode(-err);
	}
}

bool RawCephConnector::HasOmapKeys(const CephPath &path, std::error_code &ec) noexcept {
	auto ctx = RadosContext::CreateWithoutStriper(cluster, path.ns, ec);
	if (ec) {
		return false;
	}

	ec = std::error_code {};
	std::set<std::string> keys;
	if (auto err = ctx->io_ctx->omap_get_keys(path.path, "", 1, &keys); err < 0) {
		ec = RadosErrorCategory::GetErrorCode(-err);
		return false;
	}

	return !keys.empty();
}

} // namespace duckdb
