#include "cephfs.hpp"

#include "ceph_connector.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
// #include "httpfs.hpp"

#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <thread>

namespace duckdb {

static inline void ParseUrl(const string &url, string &pool_out, string &ns_out, string &path_out) {
	if (url.rfind("ceph://", 0) != 0) {
		throw IOException("URL needs to start ceph://");
	}
	auto pool_slash = url.find("//", 7);
	if (pool_slash == string::npos) {
		throw IOException("URL needs to contain a pool");
	}
	pool_out = url.substr(7, pool_slash - 7);
	if (pool_out.empty()) {
		throw IOException("URL needs to contain a non-empty pool");
	}
	auto ns_slash = url.find("//", pool_slash + 2);

	if (ns_slash == string::npos) {
		throw IOException("URL needs to contain a ns");
	}
	ns_out = url.substr(pool_slash + 2, ns_slash - pool_slash - 2);

	path_out = url.substr(ns_slash + 2);

	if (path_out.empty()) {
		throw IOException("URL needs to contain a path");
	}
}

CephFileHandle::CephFileHandle(FileSystem &fs, string path, uint8_t flags)
    : FileHandle(fs, path), flags(flags), length(0), buffer_available(0), buffer_idx(0), file_offset(0),
      buffer_start(0), buffer_end(0) {
}

// This two-phase construction allows subclasses more flexible setup.
void CephFileHandle::Initialize(FileOpener *opener) {
	// Initialize the read buffer now that we know the file exists
	ParseUrl(path, pool, ns, obj_name);
	auto &&cs = CephConnector::connnector_singleton();
	length = cs->Size(obj_name, pool, ns);
	// as we cache small files in ceph connector
	if ((flags & FileFlags::FILE_FLAGS_READ) && length > CephConnector::SMALL_FILE_THRESHOLD) {
		read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[READ_BUFFER_LEN]);
	}
}

void CephFileSystem::doReadFromCeph(FileHandle &handle, string url, idx_t file_offset, char *buffer_out,
                                    idx_t buffer_out_len) {
	auto &&cs = CephConnector::connnector_singleton();
	std::string pool, ns, path;
	ParseUrl(url, pool, ns, path);
	auto ret = cs->Read(path, pool, ns, file_offset, buffer_out, buffer_out_len);
}

unique_ptr<CephFileHandle> CephFileSystem::CreateHandle(const string &path, uint8_t flags, FileLockType lock,
                                                        FileCompressionType compression, FileOpener *opener) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);
	return duckdb::make_uniq<CephFileHandle>(*this, path, flags);
}

unique_ptr<FileHandle> CephFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                FileCompressionType compression, FileOpener *opener) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);
	auto handle = CreateHandle(path, flags, lock, compression, opener);
	handle->Initialize(opener);
	return std::move(handle);
}

// FS methods
void CephFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hfh = (CephFileHandle &)handle;

	idx_t to_read = nr_bytes;
	idx_t buffer_offset = 0;

	// Don't buffer when DirectIO is set.
	if (hfh.flags & FileFlags::FILE_FLAGS_DIRECT_IO && to_read > 0) {
		doReadFromCeph(hfh, hfh.path, location, (char *)buffer, to_read);
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location + nr_bytes;
		return;
	}

	if (location >= hfh.buffer_start && location < hfh.buffer_end) {
		hfh.file_offset = location;
		hfh.buffer_idx = location - hfh.buffer_start;
		hfh.buffer_available = (hfh.buffer_end - hfh.buffer_start) - hfh.buffer_idx;
	} else {
		// reset buffer
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location;
	}
	while (to_read > 0) {
		auto buffer_read_len = MinValue<idx_t>(hfh.buffer_available, to_read);
		if (buffer_read_len > 0) {
			D_ASSERT(hfh.buffer_start + hfh.buffer_idx + buffer_read_len <= hfh.buffer_end);
			memcpy((char *)buffer + buffer_offset, hfh.read_buffer.get() + hfh.buffer_idx, buffer_read_len);

			buffer_offset += buffer_read_len;
			to_read -= buffer_read_len;

			hfh.buffer_idx += buffer_read_len;
			hfh.buffer_available -= buffer_read_len;
			hfh.file_offset += buffer_read_len;
		}

		if (to_read > 0 && hfh.buffer_available == 0) {
			idx_t new_buffer_available = 0;
			if (hfh.read_buffer) {
				new_buffer_available = MinValue<idx_t>(hfh.READ_BUFFER_LEN, hfh.length - hfh.file_offset);
			}

			// Bypass buffer if we read more than buffer size
			if (to_read > new_buffer_available) {
				doReadFromCeph(hfh, hfh.path, location + buffer_offset, (char *)buffer + buffer_offset, to_read);
				hfh.buffer_available = 0;
				hfh.buffer_idx = 0;
				hfh.file_offset += to_read;
				break;
			} else {
				doReadFromCeph(hfh, hfh.path, hfh.file_offset, (char *)hfh.read_buffer.get(), new_buffer_available);
				hfh.buffer_available = new_buffer_available;
				hfh.buffer_idx = 0;
				hfh.buffer_start = hfh.file_offset;
				hfh.buffer_end = hfh.buffer_start + new_buffer_available;
			}
		}
	}
}

int64_t CephFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hfh = (CephFileHandle &)handle;
	idx_t max_read = hfh.length - hfh.file_offset;
	nr_bytes = MinValue<idx_t>(max_read, nr_bytes);
	Read(handle, buffer, nr_bytes, hfh.file_offset);
	return nr_bytes;
}

void CephFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("Random write for ceph files not implemented");
}

int64_t CephFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hfh = (CephFileHandle &)handle;
	// Write(handle, buffer, nr_bytes, hfh.file_offset);
	if (!(hfh.flags & FileFlags::FILE_FLAGS_WRITE)) {
		throw InternalException("Write called on file not opened in write mode");
	}
	if (hfh.file_offset != 0) {
		throw InternalException("Currently only whole writes are supported");
	}
	idx_t location = 0;
	int64_t bytes_written = 0;
	auto &&cs = CephConnector::connnector_singleton();
	while (bytes_written < nr_bytes) {
		auto curr_location = location + bytes_written;

		if (curr_location != hfh.file_offset) {
			throw InternalException("Non-sequential write not supported!");
		}

		auto bytes_to_write = nr_bytes - bytes_written;
		auto ret =
		    cs->Write(hfh.obj_name, hfh.pool, hfh.ns, hfh.file_offset, (char *)buffer + bytes_written, bytes_to_write);
		D_ASSERT(ret == bytes_to_write);
		hfh.file_offset += bytes_to_write;
		bytes_written += bytes_to_write;
	}
	return nr_bytes;
}

void CephFileSystem::FileSync(FileHandle &handle) {
}

int64_t CephFileSystem::GetFileSize(FileHandle &handle) {
	auto &sfh = (CephFileHandle &)handle;
	return sfh.length;
}

time_t CephFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &sfh = (CephFileHandle &)handle;
	return sfh.last_modified;
}

bool CephFileSystem::FileExists(const string &filename) {
	auto &&cs = CephConnector::connnector_singleton();
	string path, pool, ns;
	ParseUrl(filename, pool, ns, path);
	return cs->Exist(path, pool, ns);
}

void CephFileSystem::RemoveFile(const string &filename) {
	auto &&cs = CephConnector::connnector_singleton();
	string path, pool, ns;
	ParseUrl(filename, pool, ns, path);
	auto ret = cs->Delete(path, pool, ns);
	D_ASSERT(ret);
}
void CephFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &sfh = (CephFileHandle &)handle;
	sfh.file_offset = location;
}

bool CephFileSystem::CanHandleFile(const string &fpath) {
	auto ret = fpath.rfind("ceph://", 0) == 0;
	return ret;
}

bool CephFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                               FileOpener *opener) {
	auto &&cs = CephConnector::connnector_singleton();
	string path, pool, ns;
	ParseUrl(directory, pool, ns, path);
	auto objects = cs->ListNS(pool, ns);
	for (auto &object : objects) {
		if (object.rfind(path, 0) == 0) {
			callback(object, true);
		}
	}
	return true;
}
} // namespace duckdb