#pragma once
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class CephFileHandle : public FileHandle {
public:
	CephFileHandle(FileSystem &fs, string path, uint8_t flags);
	~CephFileHandle() override = default;
	// This two-phase construction allows subclasses more flexible setup.
	virtual void Initialize(FileOpener *opener);

	// File handle info
	uint8_t flags;
	idx_t length;
	time_t last_modified;
	bool range_read = false;

	// Read info
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;

	// Read buffer
	duckdb::unique_ptr<data_t[]> read_buffer;
	constexpr static idx_t READ_BUFFER_LEN = 1 << 20;

	string obj_name, pool, ns;

public:
	void Close() override {
	}
};

class CephFileSystem : public FileSystem {

public:
	void doReadFromCeph(FileHandle &handle, string url, idx_t file_offset, char *buffer_out, idx_t buffer_out_len);

	unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock,
	                                FileCompressionType compression, FileOpener *opener);

	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override;

	// FS methods
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void FileSync(FileHandle &handle) override;
	int64_t GetFileSize(FileHandle &handle) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	bool FileExists(const string &filename) override;
	void RemoveFile(const string &filename) override;
	void RemoveDirectory(const std::string &directory) override;
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr);
	void Seek(FileHandle &handle, idx_t location) override;
	bool CanHandleFile(const string &fpath) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	bool IsPipe(const string &filename) override {
		return false;
	}
	string GetName() const override {
		return "CephFileSystem";
	}
	friend class CephFileHandle;

protected:
	virtual duckdb::unique_ptr<CephFileHandle> CreateHandle(const string &path, uint8_t flags, FileLockType lock,
	                                                        FileCompressionType compression, FileOpener *opener);
	// CephConnector cs;
};
} // namespace duckdb