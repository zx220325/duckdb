#include "../raw_ceph_connector.hpp"
#include "ceph_connector.hpp"

#include <benchmark/benchmark.h>
#include <cstdint>
#include <gtest/gtest.h>
#include <iostream>
#include <sstream>

namespace {

constexpr auto NUM_FILES = 1'000'000;
const duckdb::CephNamespace BENCH_NAMESPACE {"tech_test", "datacore_benchmark_ns"};

std::string GetPopulateFilePath(int file_id) noexcept {
	std::ostringstream builder;
	builder << "/mbd/orders/" << file_id << ".parquet";
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
		ASSERT_EQ(write_ret, object_data.length());
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
		ASSERT_TRUE(delete_ret);
	}
}

void BenchList(benchmark::State &state) {
	auto &connector = duckdb::CephConnector::GetSingleton();

	std::uint64_t num_files_listed = 0;
	for (auto _ : state) {
		auto list = connector.ListFiles("", BENCH_NAMESPACE.pool, BENCH_NAMESPACE.ns);
		num_files_listed += list.size();
	}
	state.SetItemsProcessed(num_files_listed);
}

void BenchListRaw(benchmark::State &state) {
	auto &connector = duckdb::CephConnector::GetSingleton();
	auto &raw_connector = *connector.GetRawConnector();

	std::uint64_t num_files_listed = 0;
	for (auto _ : state) {
		std::error_code ec;
		auto list = raw_connector.ListFiles(BENCH_NAMESPACE, ec);
		num_files_listed += list.size();
	}
	state.SetItemsProcessed(num_files_listed);
}

void BenchWrite(benchmark::State &state) {
	auto &connector = duckdb::CephConnector::GetSingleton();
	std::string object_data = "example";

	for (auto _ : state) {
		auto oid = GetPopulateFilePath(NUM_FILES + state.iterations());
		auto write_ret =
		    connector.Write(oid, BENCH_NAMESPACE.pool, BENCH_NAMESPACE.ns, object_data.c_str(), object_data.length());
		benchmark::DoNotOptimize(write_ret);
	}
}

void BenchWriteRaw(benchmark::State &state) {
	auto &connector = duckdb::CephConnector::GetSingleton();
	auto &raw_connector = *connector.GetRawConnector();
	std::string object_data = "example";

	for (auto _ : state) {
		auto oid = GetPopulateFilePath(NUM_FILES + state.iterations());
		duckdb::CephPath path {BENCH_NAMESPACE, oid};

		std::error_code ec;
		auto write_ret = raw_connector.Write(path, object_data.c_str(), object_data.length(), ec);
		benchmark::DoNotOptimize(write_ret);
	}
}

} // namespace

BENCHMARK(BenchList)->Unit(benchmark::TimeUnit::kSecond);
BENCHMARK(BenchListRaw)->Unit(benchmark::TimeUnit::kSecond);
BENCHMARK(BenchWrite)->Unit(benchmark::TimeUnit::kSecond);
BENCHMARK(BenchWriteRaw)->Unit(benchmark::TimeUnit::kSecond);

int main(int argc, char **argv) {
	char arg0_default[] = "benchmark";
	char *args_default = arg0_default;
	if (!argv) {
		argc = 1;
		argv = &args_default;
	}
	::benchmark::Initialize(&argc, argv);
	if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
		return 1;
	}

	Setup();

	::benchmark::RunSpecifiedBenchmarks();
	::benchmark::Shutdown();

	Teardown();

	return 0;
}
