cmake_minimum_required(VERSION 2.8.12)

project(HTTPFsExtension)
set(CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD 17)
set(CXX_STANDARD_REQUIRED 17)

add_extension_definitions()

include_directories(
  include
  # ../parquet/include
  # ../../third_party/httplib
  # ../../third_party/openssl/include
)

set(httpfs_sources
  ceph_connector.cpp
  cephfs.cpp
  # crypto.cpp
  httpfs_extension.cpp
  # httpfs.cpp
  raw_ceph_connector.cpp
  # s3fs.cpp
  utils.cpp
)

build_static_extension(httpfs ${httpfs_sources})

set(PARAMETERS "-warnings")
build_loadable_extension(httpfs ${PARAMETERS} ${httpfs_sources})

# find_library(ssl NAMES ssl)
# find_library(crypto NAMES crypto)

# message(STATUS ${ssl})
# message(STATUS ${crypto})

find_library(rados NAMES rados)
find_library(radosstriper NAMES radosstriper)

target_link_libraries(httpfs_loadable_extension
  duckdb_mbedtls
  ${radosstriper}
  ${rados}
  # ${ssl}
  # ${crypto}
  -lstdc++fs
  -lrt
)
target_link_libraries(httpfs_extension
  duckdb_mbedtls
  ${radosstriper}
  ${rados}
  # ${ssl}
  # ${crypto}
  -lstdc++fs
  -lrt
)

# if(MINGW)
#   find_package(ZLIB)
#   target_link_libraries(httpfs_loadable_extension ZLIB::ZLIB -lcrypt32)
#   target_link_libraries(httpfs_extension ZLIB::ZLIB -lcrypt32)
# endif()

if(${BUILD_UNITTESTS})
  add_subdirectory(test)
endif()

install(
  TARGETS httpfs_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
