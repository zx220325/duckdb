#include "../raw_ceph_connector.hpp"
#include "ceph_connector.hpp"

#include <cstring>
#include <exception>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace {

constexpr auto NUM_FILES = 1'000'000;
const duckdb::CephNamespace BENCH_NAMESPACE {"tech_test", "datacore_benchmark_1m_ns"};

std::string GetPopulateFilePath(int file_id) noexcept {
    auto dir_id = file_id / 10'000;
    file_id %= 10'000;

	std::ostringstream builder;
	builder << "/" << dir_id << "/" << file_id << ".parquet";

	return builder.str();
}

void Setup() {
	auto &connector = duckdb::CephConnector::GetSingleton();
	std::string object_data = "example";

	for (auto i = 0; i < NUM_FILES; ++i) {
		if (i % 10000 == 0) {
			std::cout << "We're writing file #" << i << ". This message shows every 10000 files." << std::endl;
		}

		auto oid = GetPopulateFilePath(i);
		auto write_ret =
		    connector.Write(oid, BENCH_NAMESPACE.pool, BENCH_NAMESPACE.ns, object_data.c_str(), object_data.length());
		if (write_ret != object_data.length()) {
			throw std::runtime_error {std::string("write failed: ") + std::to_string(write_ret)};
		}
	}
}

void Teardown() {
	auto &connector = duckdb::CephConnector::GetSingleton();
	for (auto i = 0; i < NUM_FILES; ++i) {
		if (i % 10000 == 0) {
			std::cout << "We're deleting file #" << i << ". This message shows every 10000 files." << std::endl;
		}

		auto oid = GetPopulateFilePath(i);
		auto delete_ret = connector.Delete(oid, BENCH_NAMESPACE.pool, BENCH_NAMESPACE.ns);
		if (!delete_ret) {
			throw std::runtime_error {"delete failed"};
		}
	}
}

} // namespace

int main(int argc, char *argv[]) try {
	if (argc != 2) {
		std::cerr << "invalid arguments" << std::endl;
		return 1;
	}

	if (std::strcmp(argv[1], "setup") == 0) {
		Setup();
	} else if (std::strcmp(argv[1], "teardown") == 0) {
		Teardown();
	} else {
		std::cerr << "invalid command" << std::endl;
		return 1;
	}

	return 0;
} catch (const std::exception &ex) {
	std::cerr << "Error: " << ex.what() << std::endl;
	return 1;
}
