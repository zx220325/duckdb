cmake_minimum_required(VERSION 2.8.12)

project(HTTPFsExtension)

add_extension_definitions()

include_directories(include ../../third_party/httplib ../parquet/include)

build_static_extension(httpfs s3fs.cpp cephfs.cpp  ceph_connector.cpp httpfs.cpp crypto.cpp
                       httpfs_extension.cpp)
set(PARAMETERS "-warnings")
build_loadable_extension(httpfs ${PARAMETERS} s3fs.cpp cephfs.cpp  ceph_connector.cpp httpfs.cpp crypto.cpp
                         httpfs_extension.cpp)

if(MINGW)
  set(OPENSSL_USE_STATIC_LIBS TRUE)
endif()

find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})

find_library(ceph-common NAMES libceph-common.so.2)
find_library(rados NAMES librados.so.2)
find_library(radosstriper NAMES libradosstriper.so.1)

target_link_libraries(httpfs_loadable_extension duckdb_mbedtls
                      ${OPENSSL_LIBRARIES}  -lstdc++fs ${ceph-common} ${radosstriper} ${rados})
target_link_libraries(httpfs_extension duckdb_mbedtls ${OPENSSL_LIBRARIES}  -lstdc++fs ${ceph-common} ${radosstriper} ${rados})

if(MINGW)
  find_package(ZLIB)
  target_link_libraries(httpfs_loadable_extension ZLIB::ZLIB -lcrypt32)
  target_link_libraries(httpfs_extension ZLIB::ZLIB -lcrypt32)
endif()

install(
  TARGETS httpfs_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
