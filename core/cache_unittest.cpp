#include "core/cache.hpp"

#include "boost/optional.hpp"
#include "gtest/gtest.h"

namespace husky {
namespace {
class TestLRUCache : public testing::Test {
   public:
    TestLRUCache() {}
    ~TestLRUCache() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

class TestFIFOCache : public testing::Test {
   public:
    TestFIFOCache() {}
    ~TestFIFOCache() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestLRUCache, Basic) {
    LRUCache<int, int> cache(5);

    EXPECT_EQ(cache.get_size(), 0);
    EXPECT_EQ(cache.poll(), boost::none);
    EXPECT_EQ(cache.del(), boost::none);
    cache.put(1, 11);
    cache.put(2, 12);
    cache.put(3, 13);
    cache.put(4, 14);
    cache.put(5, 15);
    cache.put(6, 16);
    cache.put(7, 17);
    cache.put(8, 18);
    cache.put(9, 19);

    auto kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 5);
    EXPECT_EQ(kv_pair.second, 15);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 5);
    EXPECT_EQ(kv_pair.second, 15);

    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 6);
    EXPECT_EQ(kv_pair.second, 16);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 6);
    EXPECT_EQ(kv_pair.second, 16);

    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 7);
    EXPECT_EQ(kv_pair.second, 17);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 7);
    EXPECT_EQ(kv_pair.second, 17);

    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 8);
    EXPECT_EQ(kv_pair.second, 18);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 8);
    EXPECT_EQ(kv_pair.second, 18);

    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 9);
    EXPECT_EQ(kv_pair.second, 19);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 9);
    EXPECT_EQ(kv_pair.second, 19);

    EXPECT_EQ(cache.get_size(), 0);
}

TEST_F(TestFIFOCache, Basic) {
    FIFOCache<int, int> cache(5);

    EXPECT_EQ(cache.poll(), boost::none);
    EXPECT_EQ(cache.del(), boost::none);
    cache.put(9, 19);
    cache.put(8, 18);
    cache.put(7, 17);
    cache.put(6, 16);
    cache.put(1, 11);
    cache.put(2, 12);
    cache.put(3, 13);
    cache.put(4, 14);
    cache.put(5, 15);

    auto kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 1);
    EXPECT_EQ(kv_pair.second, 11);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 1);
    EXPECT_EQ(kv_pair.second, 11);

    cache.put(2, 12);
    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 2);
    EXPECT_EQ(kv_pair.second, 12);

    cache.put(2, 16);
    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 3);
    EXPECT_EQ(kv_pair.second, 13);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 3);
    EXPECT_EQ(kv_pair.second, 13);

    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 4);
    EXPECT_EQ(kv_pair.second, 14);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 4);
    EXPECT_EQ(kv_pair.second, 14);

    cache.put(5, 25);
    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 2);
    EXPECT_EQ(kv_pair.second, 16);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 2);
    EXPECT_EQ(kv_pair.second, 16);

    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 5);
    EXPECT_EQ(kv_pair.second, 25);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 5);
    EXPECT_EQ(kv_pair.second, 25);

    EXPECT_EQ(cache.get_size(), 0);
    EXPECT_EQ(cache.poll(), boost::none);
    EXPECT_EQ(cache.del(), boost::none);
}

}  // namespace
}  // namespace husky
