#pragma once

#include <array>
#include <cstddef>
#include <ctime>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

class RawCephConnector;

// extern const std::string CEPH_INDEX_MQ_NAME;
// (1 << 18) * (1 << 8)  64MB in total
constexpr std::size_t CEPH_INDEX_MQ_SIZE = 1 << 18;

class CephConnector {
public:
	static CephConnector &GetSingleton();

	std::int64_t Size(const std::string &path, const std::string &pool, const std::string &ns, time_t *mtm = nullptr);

	bool Exist(const std::string &path, const std::string &pool, const std::string &ns);

	std::int64_t Read(const std::string &path, const std::string &pool, const std::string &ns,
	                  std::uint64_t file_offset, char *buffer_out, std::size_t buffer_out_len);

	std::int64_t Write(const std::string &path, const std::string &pool, const std::string &ns, const char *buffer_in,
	                   std::size_t buffer_in_len);

	bool Delete(const std::string &path, const std::string &pool, const std::string &ns);

	std::vector<std::string> ListFiles(const std::string &pathprefix, const std::string &pool, const std::string &ns);

	std::time_t GetLastModifiedTime(const std::string &path, const std::string &pool, const std::string &ns);

	void RefreshFileIndex(const std::string &pool, const std::string &ns);

	void ClearCache();
	void DisableCache();

	RawCephConnector *GetRawConnector() const noexcept {
		return raw.get();
	}

private:
	class FileIndexManager;
	class FileMetaManager;

	CephConnector();

	~CephConnector() noexcept;

	CephConnector(const CephConnector &) = delete;
	CephConnector &operator=(const CephConnector &) = delete;

	std::unique_ptr<RawCephConnector> raw;
	std::unique_ptr<FileIndexManager> index_manager;
	std::unique_ptr<FileMetaManager> meta_manager;
};

} // namespace duckdb
