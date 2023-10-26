#include "../raw_ceph_connector.hpp"
#include "ceph_connector.hpp"

#include <benchmark/benchmark.h>
#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>
#include <iostream>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace {

constexpr const char BENCH_POOL_NAME[] = "tech_test";
constexpr const char BENCH_NAMESPACE_NAME[] = "datacore_benchmark_ns";
constexpr const char BENCH_1M_NAMESPACE_NAME[] = "datacore_benchmark_1m_ns";

} // namespace

class CephConnectorBenchmark : public benchmark::Fixture {
public:
	void SetUp(const benchmark::State &) override {
		connector = &duckdb::CephConnector::GetSingleton();
		new_files.clear();
	}

	void TearDown(const benchmark::State &) override {
		if (!connector) {
			return;
		}

		for (const auto &f : new_files) {
			if (use_raw_connector) {
				std::error_code ec;
				connector->GetRawConnector()->Delete(f, ec);
			} else {
				connector->Delete(f.path, f.ns.pool, f.ns.ns);
			}
		}

		new_files.clear();
	}

protected:
	duckdb::CephConnector *connector {nullptr};
	bool use_raw_connector {false};
	std::vector<duckdb::CephPath> new_files {};
};

BENCHMARK_DEFINE_F(CephConnectorBenchmark, BenchList)(benchmark::State &state) {
	std::uint64_t num_files_listed = 0;
	for (auto _ : state) {
		auto list = connector->ListFiles("/0", BENCH_POOL_NAME, BENCH_1M_NAMESPACE_NAME);
		num_files_listed += list.size();
	}
	state.counters["files"] = num_files_listed;
}

BENCHMARK_DEFINE_F(CephConnectorBenchmark, BenchListRaw)(benchmark::State &state) {
	std::uint64_t num_files_listed = 0;
	for (auto _ : state) {
		std::error_code ec;
		auto list = connector->GetRawConnector()->ListFilesAndFilter(
		    duckdb::CephNamespace {BENCH_POOL_NAME, BENCH_1M_NAMESPACE_NAME},
		    [](const std::string &oid, std::error_code &) -> bool {
			    constexpr const char PREFIX[] = "/0";
			    return oid.length() >= 2 && std::strncmp(oid.c_str(), PREFIX, 2) == 0;
		    },
		    ec);
		num_files_listed += list.size();
	}
	state.counters["files"] = num_files_listed;
}

BENCHMARK_DEFINE_F(CephConnectorBenchmark, BenchWrite)(benchmark::State &state) {
	std::string object_data = "example";

	std::uint64_t round = 0;
	for (auto _ : state) {
		auto oid = std::string("/test/") + std::to_string(round) + ".parquet";
		auto write_ret =
		    connector->Write(oid, BENCH_POOL_NAME, BENCH_NAMESPACE_NAME, object_data.c_str(), object_data.length());
		benchmark::DoNotOptimize(write_ret);

		new_files.push_back(duckdb::CephPath {{BENCH_POOL_NAME, BENCH_NAMESPACE_NAME}, oid});

		++round;
	}
}

BENCHMARK_DEFINE_F(CephConnectorBenchmark, BenchWriteRaw)(benchmark::State &state) {
	use_raw_connector = true;
	std::string object_data = "example";

	std::uint64_t round = 0;
	for (auto _ : state) {
		auto oid = std::string("/test/") + std::to_string(round) + ".parquet";
		duckdb::CephPath path {{BENCH_POOL_NAME, BENCH_NAMESPACE_NAME}, oid};

		std::error_code ec;
		auto write_ret = connector->GetRawConnector()->Write(path, object_data.c_str(), object_data.length(), ec);
		benchmark::DoNotOptimize(write_ret);

		new_files.push_back(std::move(path));

		++round;
	}
}

BENCHMARK_REGISTER_F(CephConnectorBenchmark, BenchList)->Unit(benchmark::TimeUnit::kSecond);
BENCHMARK_REGISTER_F(CephConnectorBenchmark, BenchListRaw)->Unit(benchmark::TimeUnit::kSecond);
BENCHMARK_REGISTER_F(CephConnectorBenchmark, BenchWrite)->Unit(benchmark::TimeUnit::kSecond);
BENCHMARK_REGISTER_F(CephConnectorBenchmark, BenchWriteRaw)->Unit(benchmark::TimeUnit::kSecond);

BENCHMARK_MAIN();
