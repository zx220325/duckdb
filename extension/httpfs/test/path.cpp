#include "../utils.hpp"

#include <gtest/gtest.h>
#include <string>
#include <vector>

TEST(PathTest, ParseAbsolutePath) {
    duckdb::Path path {"/a/b/c.parquet"};
    std::vector<std::string> expected_components {"/", "a/", "b/", "c.parquet"};
    ASSERT_EQ(path.GetComponents(), expected_components);
}

TEST(PathTest, ParseRelativePath) {
    duckdb::Path path {"a/b/c.parquet"};
    std::vector<std::string> expected_components {"a/", "b/", "c.parquet"};
    ASSERT_EQ(path.GetComponents(), expected_components);
}

TEST(PathTest, ParseDirectoryName) {
    duckdb::Path path {"/a/b/c.parquet/"};
    std::vector<std::string> expected_components {"/", "a/", "b/", "c.parquet/"};
    ASSERT_EQ(path.GetComponents(), expected_components);
}

TEST(PathTest, ParseConsecutiveSlashes) {
    duckdb::Path path {"a///b/c.parquet"};
    std::vector<std::string> expected_components {"a/", "/", "/", "b/", "c.parquet"};
    ASSERT_EQ(path.GetComponents(), expected_components);
}
