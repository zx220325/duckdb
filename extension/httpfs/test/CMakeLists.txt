include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)

add_executable(ceph_test ceph.cpp)
target_link_libraries(ceph_test httpfs_extension GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(ceph_test)
