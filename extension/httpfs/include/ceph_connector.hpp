#pragma once

#include "radosstriper/libradosstriper.hpp"

#include <cstdlib>
#include <memory>
#include <string>

namespace duckdb {

class CephConnector {

public:
	static std::shared_ptr<CephConnector> connnector_singleton() {
		static std::shared_ptr<CephConnector> instance(new CephConnector);
		auto cur_pid = getpid();
		if (cur_pid != pid_) {
			instance->init();
			pid_ = cur_pid;
		}
		return instance;
	}

	[[nodiscard]] int64_t Size(const std::string &path, const std::string &pool, const std::string &ns);

	[[nodiscard]] bool Exist(const std::string &path, const std::string &pool, const std::string &ns);

	[[nodiscard]] int64_t Read(const std::string &path, const std::string &pool, const std::string &ns,
	                           int64_t file_offset, char *buffer_out, int64_t buffer_out_len);

	[[nodiscard]] int64_t Write(const std::string &buf, const std::string &path, const std::string &pool,
	                            const std::string &ns);

	[[nodiscard]] bool Delete(const std::string &path, const std::string &pool, const std::string &ns);

private:
	CephConnector() = default;
	CephConnector(const CephConnector &) = delete;
	CephConnector &operator=(const CephConnector &) = delete;
	void init();
	struct CombStriper {
		std::shared_ptr<librados::IoCtx> io_ctx;
		std::shared_ptr<libradosstriper::RadosStriper> rs;
	};
	std::shared_ptr<CombStriper> getCombStriper(const std::string &pool, const std::string &ns);
	std::unique_ptr<librados::Rados> cluster;
	static pid_t pid_;
};
} // namespace duckdb