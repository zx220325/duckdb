find_package(GTest REQUIRED)
find_package(benchmark REQUIRED)

add_executable(ceph_test ceph.cpp path.cpp)
target_link_libraries(ceph_test httpfs_extension GTest::gtest_main)

add_executable(ceph_bench_setup ceph_bench_setup.cpp)
target_link_libraries(ceph_bench_setup httpfs_extension)

add_executable(ceph_bench ceph_bench.cpp)
target_link_libraries(ceph_bench httpfs_extension benchmark::benchmark GTest::gtest)

include(GoogleTest)
gtest_discover_tests(ceph_test)
