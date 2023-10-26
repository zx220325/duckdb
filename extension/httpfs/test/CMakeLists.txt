include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_Declare(
  benchmark
  URL https://github.com/google/benchmark/archive/refs/tags/v1.8.3.zip
)
FetchContent_MakeAvailable(googletest benchmark)

add_executable(ceph_test ceph.cpp)
target_link_libraries(ceph_test httpfs_extension GTest::gtest_main)

add_executable(ceph_bench_setup ceph_bench_setup.cpp)
target_link_libraries(ceph_bench_setup httpfs_extension)

add_executable(ceph_bench ceph_bench.cpp)
target_link_libraries(ceph_bench httpfs_extension benchmark::benchmark GTest::gtest)

include(GoogleTest)
gtest_discover_tests(ceph_test)
