#include "../raw_ceph_connector.hpp"
#include "ceph_connector.hpp"

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace {

const duckdb::CephNamespace TEST_NAMESPACE {"tech_test", "datacore_test_ns"};

void FillRandomBytes(char *buffer, std::size_t buffer_size) noexcept {
	std::minstd_rand engine(std::chrono::high_resolution_clock::now().time_since_epoch().count());
	std::uniform_int_distribution<std::uint8_t> byte_dist;
	for (std::size_t i = 0; i < buffer_size; ++i) {
		buffer[i] = static_cast<char>(byte_dist(engine));
	}
}

} // namespace

class CephConnectorTest : public testing::Test {
protected:
	duckdb::CephConnector *connector {nullptr};

	void SetUp() override {
		connector = &duckdb::CephConnector::GetSingleton();
		ClearTestNamespace();
		connector->ClearCache();
	}

	void TearDown() override {
		ClearTestNamespace();
	}

	void CheckIndex(const std::string &index_oid, const std::set<std::string> &expected_keys) {
		std::error_code ec;
		auto kv = connector->GetRawConnector()->GetOmapKv(duckdb::CephPath {TEST_NAMESPACE, index_oid}, {}, ec);
		ASSERT_FALSE(ec);

		std::set<std::string> actual_keys;
		for (const auto &[key, value] : kv) {
			actual_keys.insert(key);
		}

		ASSERT_EQ(expected_keys, actual_keys);
	}

private:
	void ClearTestNamespace() {
		if (!connector) {
			return;
		}

		auto raw_connector = connector->GetRawConnector();

		std::error_code ec;
		auto oid_list = raw_connector->ListFiles(TEST_NAMESPACE, ec);
		if (ec) {
			throw std::runtime_error {"ClearNamespace: cannot list files"};
		}

		for (const auto &oid : oid_list) {
			raw_connector->RadosDelete(duckdb::CephPath {TEST_NAMESPACE, oid}, ec);
			if (ec) {
				throw std::runtime_error {"ClearNamespace: cannot delete files"};
			}
		}
	}
};

TEST_F(CephConnectorTest, WriteAndRead) {
	std::string oid = "/test.parquet";
	std::string data = "hello";

	std::error_code ec;
	auto ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
	ASSERT_EQ(ret, data.length());

	CheckIndex(".ceph_index", {"/"});
	CheckIndex("/.ceph_index", {"test.parquet"});

	std::string buffer(data.length(), 0);
	ret = connector->Read(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, 0, buffer.data(), buffer.length());
	ASSERT_EQ(ret, buffer.length());
	ASSERT_EQ(buffer, data);
}

TEST_F(CephConnectorTest, WriteAndReadLargeFile) {
	constexpr std::size_t FILE_SIZE = 1024 * 1024; // 1MB file.

	std::string oid = "/test.parquet";
	std::string data(FILE_SIZE, 0);
	FillRandomBytes(data.data(), data.length());

	std::error_code ec;
	auto ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
	ASSERT_EQ(ret, data.length());

	constexpr std::size_t READ_SIZE = 64;
	static_assert(READ_SIZE <= FILE_SIZE);

	std::string buffer(READ_SIZE, 0);
	auto read_offset = FILE_SIZE - READ_SIZE;
	ret = connector->Read(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, read_offset, buffer.data(),
	                      buffer.length());
	ASSERT_EQ(ret, buffer.length());

	auto cmp = std::memcmp(buffer.data(), data.data() + read_offset, READ_SIZE);
	ASSERT_EQ(cmp, 0);
}

TEST_F(CephConnectorTest, OverwriteAndRead) {
	std::string oid = "/test.parquet";
	std::string old_data = "old";

	std::error_code ec;
	auto ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, old_data.c_str(), old_data.length());
	ASSERT_EQ(ret, old_data.length());

	std::string buffer(old_data.length(), 0);
	ret = connector->Read(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, 0, buffer.data(), buffer.length());
	ASSERT_EQ(ret, buffer.length());
	ASSERT_EQ(buffer, old_data);

	std::string new_data = "new";
	ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, new_data.c_str(), new_data.length());
	ASSERT_EQ(ret, new_data.length());

	buffer.resize(new_data.length());
	ret = connector->Read(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, 0, buffer.data(), buffer.length());
	ASSERT_EQ(ret, buffer.length());
	ASSERT_EQ(buffer, new_data);
}

TEST_F(CephConnectorTest, ReadWriteEmptyFile) {
	std::string oid = "/test.parquet";
	std::string data = "";

	std::error_code ec;
	auto ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
	ASSERT_EQ(ret, 0);

	CheckIndex(".ceph_index", {"/"});
	CheckIndex("/.ceph_index", {"test.parquet"});

	data.resize(4);
	ret = connector->Read(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, 0, data.data(), data.length());
	ASSERT_EQ(ret, 0);
}

TEST_F(CephConnectorTest, GetSize) {
	std::string oid = "/test.parquet";
	std::string data = "hello";

	std::error_code ec;
	auto ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
	ASSERT_EQ(ret, data.length());

	auto sz = connector->Size(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_EQ(sz, data.length());
}

TEST_F(CephConnectorTest, GetSizeEmptyFile) {
	std::string oid = "/test.parquet";
	std::string data = "";

	std::error_code ec;
	auto ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
	ASSERT_EQ(ret, data.length());

	auto sz = connector->Size(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_EQ(sz, data.length());
}

TEST_F(CephConnectorTest, GetSizeNonExist) {
	std::string oid = "/test.parquet";
	std::string data = "hello";

	auto sz = connector->Size(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_EQ(sz, -ENOENT);
}

TEST_F(CephConnectorTest, Exist) {
	std::string data = "hello";

	std::error_code ec;
	auto ret = connector->Write("/test.parquet", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
	ASSERT_EQ(ret, data.length());

	ret = connector->Write("/test_empty.parquet", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, "", 0);
	ASSERT_EQ(ret, 0);

	auto exist = connector->Exist("/nonexist.parquet", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_FALSE(exist);

	exist = connector->Exist("/test.parquet", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_TRUE(exist);

	exist = connector->Exist("/test_empty.parquet", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_TRUE(exist);
}

TEST_F(CephConnectorTest, ReadNonExist) {
	std::string oid = "/test.parquet";
	std::string buffer(1024, 0);
	auto ret = connector->Read(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, 0, buffer.data(), buffer.length());
	ASSERT_EQ(ret, -ENOENT);
}

TEST_F(CephConnectorTest, Delete) {
	std::string oid = "/test.parquet";
	std::string data = "hello";

	std::error_code ec;
	auto write_ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
	ASSERT_EQ(write_ret, data.length());

	auto delete_ret = connector->Delete(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_TRUE(delete_ret);

	std::string buffer(data.length(), 0);
	auto read_ret = connector->Read(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, 0, buffer.data(), buffer.length());
	ASSERT_EQ(read_ret, -ENOENT);

	auto index_exist = connector->GetRawConnector()->RadosExist(duckdb::CephPath {TEST_NAMESPACE, "/.ceph_index"}, ec);
	ASSERT_FALSE(ec);
	ASSERT_FALSE(index_exist);
}

TEST_F(CephConnectorTest, DeleteNonExist) {
	std::string oid = "/test.parquet";
	std::string data = "hello";

	auto delete_ret = connector->Delete(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_TRUE(delete_ret);
}

TEST_F(CephConnectorTest, RefreshFileIndex) {
	// Create multiple objects in Ceph using the raw connector interface to avoid the creation of indexes.
	std::vector<std::string> oid_list {"/mbd/orders/a.parquet", "/mbd/orders/b.parquet", "/mbd/trades/a.parquet",
	                                   "/mbd/trades/b.parquet"};
	for (const auto &oid : oid_list) {
		duckdb::CephPath object_path {TEST_NAMESPACE, oid};
		std::string data = "hello";
		std::error_code ec;
		connector->GetRawConnector()->Write(object_path, data.c_str(), data.length(), ec);
		ASSERT_FALSE(ec);
	}

	connector->RefreshFileIndex(TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);

	CheckIndex("/mbd/orders/.ceph_index", {"a.parquet", "b.parquet"});
	CheckIndex("/mbd/trades/.ceph_index", {"a.parquet", "b.parquet"});
	CheckIndex("/mbd/.ceph_index", {"orders/", "trades/"});
	CheckIndex("/.ceph_index", {"mbd/"});
	CheckIndex(".ceph_index", {"/"});
}

TEST_F(CephConnectorTest, ListFiles) {
	std::vector<std::string> oid_list {"/mbd/orders/a.parquet", "/mbd/orders/b.parquet", "/mbd/trades/a.parquet",
	                                   "/mbd/trades/b.parquet"};
	for (const auto &oid : oid_list) {
		duckdb::CephPath object_path {TEST_NAMESPACE, oid};
		std::string data = "hello";
		auto write_ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
		ASSERT_EQ(write_ret, data.length());
	}

	CheckIndex("/mbd/orders/.ceph_index", {"a.parquet", "b.parquet"});
	CheckIndex("/mbd/trades/.ceph_index", {"a.parquet", "b.parquet"});
	CheckIndex("/mbd/.ceph_index", {"orders/", "trades/"});
	CheckIndex("/.ceph_index", {"mbd/"});
	CheckIndex(".ceph_index", {"/"});

	auto files_list = connector->ListFiles("", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);

	std::set<std::string> file_names;
	for (const auto &f : files_list) {
		file_names.insert(f);
	}

	std::set<std::string> expected_file_names {"/mbd/orders/a.parquet", "/mbd/orders/b.parquet",
	                                           "/mbd/trades/a.parquet", "/mbd/trades/b.parquet"};
	ASSERT_EQ(expected_file_names, file_names);
}

TEST_F(CephConnectorTest, ListFilesWithPrefix) {
	std::vector<std::string> oid_list {"/mbd/orders/a.parquet", "/mbd/orders/b.parquet", "/mbd/trades/a.parquet",
	                                   "/mbd/trades/b.parquet"};
	for (const auto &oid : oid_list) {
		duckdb::CephPath object_path {TEST_NAMESPACE, oid};
		std::string data = "hello";
		auto write_ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
		ASSERT_EQ(write_ret, data.length());
	}

	auto files_list = connector->ListFiles("/mbd/or", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);

	std::set<std::string> file_names;
	for (const auto &f : files_list) {
		file_names.insert(f);
	}

	std::set<std::string> expected_file_names {"/mbd/orders/a.parquet", "/mbd/orders/b.parquet"};
	ASSERT_EQ(expected_file_names, file_names);
}

TEST_F(CephConnectorTest, ListFilesNoIndex) {
	// Create multiple objects in Ceph using the raw connector interface to avoid the creation of indexes.
	std::vector<std::string> oid_list {"/mbd/orders/a.parquet", "/mbd/orders/b.parquet", "/mbd/trades/a.parquet",
	                                   "/mbd/trades/b.parquet"};
	for (const auto &oid : oid_list) {
		duckdb::CephPath object_path {TEST_NAMESPACE, oid};
		std::string data = "hello";
		std::error_code ec;
		connector->GetRawConnector()->Write(object_path, data.c_str(), data.length(), ec);
		ASSERT_FALSE(ec);
	}

	// Without index, no files could be listed using the ListFiles interface.

	auto files_list = connector->ListFiles("/mbd/or", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_TRUE(files_list.empty());
}

TEST_F(CephConnectorTest, ListFilesAfterDelete) {
	std::vector<std::string> oid_list {"/mbd/orders/a.parquet", "/mbd/orders/b.parquet", "/mbd/trades/a.parquet",
	                                   "/mbd/trades/b.parquet"};
	for (const auto &oid : oid_list) {
		duckdb::CephPath object_path {TEST_NAMESPACE, oid};
		std::string data = "hello";
		auto write_ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
		ASSERT_EQ(write_ret, data.length());
	}

	auto delete_ret = connector->Delete("/mbd/trades/b.parquet", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_TRUE(delete_ret);

	CheckIndex("/mbd/orders/.ceph_index", {"a.parquet", "b.parquet"});
	CheckIndex("/mbd/trades/.ceph_index", {"a.parquet"});
	CheckIndex("/mbd/.ceph_index", {"orders/", "trades/"});
	CheckIndex("/.ceph_index", {"mbd/"});
	CheckIndex(".ceph_index", {"/"});

	auto files_list = connector->ListFiles("", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);

	std::set<std::string> file_names;
	for (const auto &f : files_list) {
		file_names.insert(f);
	}

	std::set<std::string> expected_file_names {"/mbd/orders/a.parquet", "/mbd/orders/b.parquet",
	                                           "/mbd/trades/a.parquet"};
	ASSERT_EQ(expected_file_names, file_names);
}

TEST_F(CephConnectorTest, ListFilesAfterReplace) {
	std::string data = "hello";
	std::vector<std::string> oid_list {"/mbd/orders/a.parquet", "/mbd/orders/b.parquet", "/mbd/trades/a.parquet",
	                                   "/mbd/trades/b.parquet"};
	for (const auto &oid : oid_list) {
		duckdb::CephPath object_path {TEST_NAMESPACE, oid};
		auto write_ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
		ASSERT_EQ(write_ret, data.length());
	}

	auto delete_ret = connector->Delete("/mbd/trades/b.parquet", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_TRUE(delete_ret);

	auto write_ret =
	    connector->Write("/mbd/trades/b.parquet", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
	ASSERT_EQ(write_ret, data.length());

	auto files_list = connector->ListFiles("", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);

	std::set<std::string> file_names;
	for (const auto &f : files_list) {
		file_names.insert(f);
	}

	std::set<std::string> expected_file_names {"/mbd/orders/a.parquet", "/mbd/orders/b.parquet",
	                                           "/mbd/trades/a.parquet", "/mbd/trades/b.parquet"};
	ASSERT_EQ(expected_file_names, file_names);
}

TEST_F(CephConnectorTest, ListFilesNonRegularPaths) {
	std::string data = "hello";
	std::vector<std::string> oid_list {"mbd//orders/a.parquet", "/mbd/orders/b.parquet/", "/mbd/trades////a.parquet",
	                                   "mbd/trades/b.parquet///"};
	for (const auto &oid : oid_list) {
		duckdb::CephPath object_path {TEST_NAMESPACE, oid};
		auto write_ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
		ASSERT_EQ(write_ret, data.length());
	}

	CheckIndex(".ceph_index", {"/", "mbd/"});
	CheckIndex("mbd/.ceph_index", {"/", "trades/"});
	CheckIndex("mbd/trades/.ceph_index", {"b.parquet/"});
	CheckIndex("mbd/trades/b.parquet/.ceph_index", {"/"});
	CheckIndex("mbd/trades/b.parquet//.ceph_index", {"/"});
	CheckIndex("mbd//.ceph_index", {"orders/"});
	CheckIndex("mbd//orders/.ceph_index", {"a.parquet"});
	CheckIndex("/.ceph_index", {"mbd/"});
	CheckIndex("/mbd/.ceph_index", {"orders/", "trades/"});
	CheckIndex("/mbd/orders/.ceph_index", {"b.parquet/"});
	CheckIndex("/mbd/trades/.ceph_index", {"/"});
	CheckIndex("/mbd/trades//.ceph_index", {"/"});
	CheckIndex("/mbd/trades///.ceph_index", {"/"});
	CheckIndex("/mbd/trades////.ceph_index", {"a.parquet"});

	auto files_list = connector->ListFiles("", TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);

	std::set<std::string> file_names;
	for (const auto &f : files_list) {
		file_names.insert(f);
	}

	std::set<std::string> expected_file_names {"mbd//orders/a.parquet", "/mbd/orders/b.parquet/",
	                                           "/mbd/trades////a.parquet", "mbd/trades/b.parquet///"};
	ASSERT_EQ(expected_file_names, file_names);
}

TEST_F(CephConnectorTest, GetLastModifiedTime) {
	std::string oid = "/test.parquet";
	std::string data = "hello";

	std::error_code ec;
	auto write_ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
	ASSERT_EQ(write_ret, data.length());

	auto modified_time = connector->GetLastModifiedTime(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_GT(modified_time, 0);

	for (auto i = 0; i < 3; ++i) {
		auto read_ret = connector->Read(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, 0, data.data(), data.length());
		ASSERT_EQ(read_ret, data.length());

		std::this_thread::sleep_for(std::chrono::seconds {1});
		connector->ClearCache();
	}

	auto now_modified_time = connector->GetLastModifiedTime(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_EQ(modified_time, now_modified_time);

	write_ret = connector->Write(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns, data.c_str(), data.length());
	ASSERT_EQ(write_ret, data.length());

	auto updated_modified_time = connector->GetLastModifiedTime(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_GT(updated_modified_time, modified_time);
}

TEST_F(CephConnectorTest, GetLastModifiedTimeNonExist) {
	std::string oid = "/test.parquet";
	std::string data = "hello";

	auto modified_time = connector->GetLastModifiedTime(oid, TEST_NAMESPACE.pool, TEST_NAMESPACE.ns);
	ASSERT_EQ(modified_time, -ENOENT);
}
