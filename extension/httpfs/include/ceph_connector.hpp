#pragma once

#include "LRUCache11.hpp"
#include "radosstriper/libradosstriper.hpp"

#include <array>
#include <cstdlib>
#include <memory>
#include <mutex>
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

	[[nodiscard]] int64_t Write(const std::string &path, const std::string &pool, const std::string &ns,
	                            int64_t file_offset, char *buffer_in, int64_t buffer_in_len);

	[[nodiscard]] bool Delete(const std::string &path, const std::string &pool, const std::string &ns);

	static constexpr int64_t SMALL_FILE_THRESHOLD = 4 << 20;

private:
	[[nodiscard]] size_t BKDRHash(const std::string &path, const std::string &pool, const std::string &ns);
	int64_t doRead(const std::string &path, const std::string &pool, const std::string &ns, int64_t file_offset,
	               char *buffer_out, int64_t buffer_out_len);

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
	lru11::Cache<size_t, int64_t, std::mutex> meta_cache {1024};
	lru11::Cache<size_t, std::shared_ptr<std::vector<char>>, std::mutex> small_files_cache {128};

	static pid_t pid_;
};
} // namespace duckdb