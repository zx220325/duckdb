#include "cephfs.hpp"

#include "ceph_connector.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "utils.hpp"

#include <chrono>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <utility>

namespace duckdb {

CephFileHandle::CephFileHandle(FileSystem &fs, string path, uint8_t flags)
    : FileHandle(fs, std::move(path)), flags(flags), length(0), buffer_available(0), buffer_idx(0), file_offset(0),
      buffer_start(0), buffer_end(0) {
}

// This two-phase construction allows subclasses more flexible setup.
void CephFileHandle::Initialize(FileOpener *opener) {
	// Initialize the read buffer now that we know the file exists
	ParseUrl(path, pool, ns, obj_name);
	auto &&cs = CephConnector::GetSingleton();

	std::time_t last_modified;
	length = cs.Size(obj_name, pool, ns, &last_modified);

	// as we cache small files in ceph connector
	if (flags & FileFlags::FILE_FLAGS_READ) {
		read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[READ_BUFFER_LEN]);
	}
}

int64_t CephFileSystem::DoReadFromCeph(FileHandle &handle, const string &url, idx_t file_offset, char *buffer_out,
                                       idx_t buffer_out_len) {
	std::string pool, ns, path;
	ParseUrl(url, pool, ns, path);
	auto &&cs = CephConnector::GetSingleton();
	auto ret = cs.Read(path, pool, ns, file_offset, buffer_out, buffer_out_len);
	return ret;
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
	auto &hfh = static_cast<CephFileHandle &>(handle);

	idx_t to_read = nr_bytes;
	idx_t buffer_offset = 0;

	// Don't buffer when DirectIO is set.
	if (hfh.flags & FileFlags::FILE_FLAGS_DIRECT_IO && to_read > 0) {
		auto ret = DoReadFromCeph(hfh, hfh.path, location, reinterpret_cast<char *>(buffer), to_read);
		if (ret < 0) {
			throw std::runtime_error(std::string("failed when read from ceph, ") + strerror(-static_cast<int>(ret)));
		}
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
			memcpy(reinterpret_cast<char *>(buffer) + buffer_offset, hfh.read_buffer.get() + hfh.buffer_idx,
			       buffer_read_len);

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
				auto ret = DoReadFromCeph(hfh, hfh.path, location + buffer_offset,
				                          reinterpret_cast<char *>(buffer) + buffer_offset, to_read);
				if (ret < 0) {
					throw std::runtime_error(std::string("failed when read from ceph, ") +
					                         strerror(-static_cast<int>(ret)));
				}
				hfh.buffer_available = 0;
				hfh.buffer_idx = 0;
				hfh.file_offset += to_read;
				break;
			} else {
				auto ret = DoReadFromCeph(hfh, hfh.path, hfh.file_offset,
				                          reinterpret_cast<char *>(hfh.read_buffer.get()), new_buffer_available);
				if (ret < 0) {
					throw std::runtime_error(std::string("failed when read from ceph, ") +
					                         strerror(-static_cast<int>(ret)));
				}
				hfh.buffer_available = new_buffer_available;
				hfh.buffer_idx = 0;
				hfh.buffer_start = hfh.file_offset;
				hfh.buffer_end = hfh.buffer_start + new_buffer_available;
			}
		}
	}
}

int64_t CephFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hfh = static_cast<CephFileHandle &>(handle);
	if (hfh.length < 0) {
		return hfh.length;
	}
	idx_t max_read = hfh.length - hfh.file_offset;
	nr_bytes = MinValue<idx_t>(max_read, nr_bytes);
	Read(handle, buffer, nr_bytes, hfh.file_offset);
	return nr_bytes;
}

void CephFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("Random write for ceph files not implemented");
	/*
	while (bytes_written < nr_bytes) {
	    auto curr_location = location + bytes_written;

	    if (curr_location != hfh.file_offset) {
	        throw InternalException("Non-sequential write not supported!");
	    }

	    auto bytes_to_write = nr_bytes - bytes_written;
	    auto ret =
	        cs.Write(hfh.obj_name, hfh.pool, hfh.ns, hfh.file_offset, (char *)buffer + bytes_written, bytes_to_write);
	    D_ASSERT(ret == bytes_to_write);
	    hfh.file_offset += bytes_to_write;
	    bytes_written += bytes_to_write;
	}
	return nr_bytes;
	*/
}

int64_t CephFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hfh = static_cast<CephFileHandle &>(handle);
	if (!(hfh.flags & FileFlags::FILE_FLAGS_WRITE)) {
		throw InternalException("Write called on file not opened in write mode");
	}
	if (hfh.file_offset != 0) {
		throw InternalException("Currently only whole writes are supported");
	}
	auto &&cs = CephConnector::GetSingleton();
	return cs.Write(hfh.obj_name, hfh.pool, hfh.ns, reinterpret_cast<const char *>(buffer), nr_bytes);
}

void CephFileSystem::FileSync(FileHandle &handle) {
}

int64_t CephFileSystem::GetFileSize(FileHandle &handle) {
	auto &sfh = static_cast<CephFileHandle &>(handle);
	return sfh.length;
}

time_t CephFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &cfh = static_cast<CephFileHandle &>(handle);

	std::string path, pool, ns;
	ParseUrl(cfh.path, pool, ns, path);

	auto &cs = CephConnector::GetSingleton();
	return cs.GetLastModifiedTime(path, pool, ns);
}

bool CephFileSystem::FileExists(const string &filename) {
	string path, pool, ns;
	ParseUrl(filename, pool, ns, path);
	auto &&cs = CephConnector::GetSingleton();
	return cs.Exist(path, pool, ns);
}

void CephFileSystem::RemoveFile(const string &filename) {
	string path, pool, ns;
	ParseUrl(filename, pool, ns, path);
	auto &&cs = CephConnector::GetSingleton();
	auto ret = cs.Delete(path, pool, ns);
	if (!ret) {
		throw std::runtime_error("delete " + filename + " failed");
	}
	D_ASSERT(ret);
}

void CephFileSystem::RemoveDirectory(const std::string &directory) {
	string path, pool, ns;
	ParseUrl(directory, pool, ns, path);
	auto &&cs = CephConnector::GetSingleton();
	for (auto &obj : cs.ListFiles(path, pool, ns)) {
		if (!cs.Delete(obj, pool, ns)) {
			throw std::runtime_error("delete " + directory + " failed");
		}
	}
}

void CephFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &sfh = static_cast<CephFileHandle &>(handle);
	sfh.file_offset = location;
}

bool CephFileSystem::CanHandleFile(const string &fpath) {
	auto ret = fpath.rfind("ceph://", 0) == 0;
	return ret;
}

bool CephFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                               FileOpener *opener) {
	if (directory == "ceph://persist_index") {
		return true;
	}

	auto &&cs = CephConnector::GetSingleton();
	string path, pool, ns;
	ParseUrl(directory, pool, ns, path);

	if (path == "refresh_index") {
		cs.RefreshFileIndex(pool, ns);
		return true;
	}
	for (auto &object : cs.ListFiles(path, pool, ns)) {
		callback("ceph://" + pool + "//" + ns + "/" + object, false);
	}
	return true;
}

vector<string> CephFileSystem::Glob(const string &filename, FileOpener *opener) {
	// AWS matches on prefix, not glob pattern, so we take a substring until the first wildcard char for the aws calls
	string path, pool, ns;
	ParseUrl(filename, pool, ns, path);

	auto first_wildcard_pos = path.find_first_of("?*[\\");
	if (first_wildcard_pos == string::npos) {
		if (FileExists(filename)) {
			return {filename};
		}
		return {};
	}

	string shared_path_prefix = path.substr(0, first_wildcard_pos);
	vector<string> ret;
	auto &&cs = CephConnector::GetSingleton();
	for (auto &obj : cs.ListFiles(shared_path_prefix, pool, ns)) {
		if (LikeFun::Glob(obj.c_str(), obj.size(), path.c_str(), path.size())) {
			ret.push_back("ceph://" + pool + "//" + ns + "/" + obj);
		}
	}
	return ret;
}
} // namespace duckdb