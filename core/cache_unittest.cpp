#include "core/cache.hpp"

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

    EXPECT_EQ(cache.poll(), boost::none);
    EXPECT_EQ(cache.del(), boost::none);
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

    cache.put(2, 16);
    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 3);
    EXPECT_EQ(kv_pair.second, 13);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 3);
    EXPECT_EQ(kv_pair.second, 13);

    cache.put(4, 17);
    cache.put(5, 18);

    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 2);
    EXPECT_EQ(kv_pair.second, 16);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 2);
    EXPECT_EQ(kv_pair.second, 16);

    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 4);
    EXPECT_EQ(kv_pair.second, 17);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 4);
    EXPECT_EQ(kv_pair.second, 17);

    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 5);
    EXPECT_EQ(kv_pair.second, 18);

    kv_pair = *cache.del();
    EXPECT_EQ(kv_pair.first, 5);
    EXPECT_EQ(kv_pair.second, 18);

    EXPECT_EQ(cache.get_size(), 0);

    cache.put(1, 100);
    cache.put(2, 200);
    cache.put(3, 300);
    cache.put(4, 400);
    cache.put(5, 500);
    EXPECT_EQ(cache.get_size(), 5);

    cache.put(6, 600);
    EXPECT_EQ(cache.get_size(), 5);
    cache.put(7, 700);
    cache.put(8, 800);
    cache.put(9, 900);
    cache.put(10, 1000);
    EXPECT_EQ(cache.get_size(), 5);

    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 6);
    EXPECT_EQ(kv_pair.second, 600);
}

TEST_F(TestFIFOCache, Basic) {
    FIFOCache<int, int> cache(5);

    EXPECT_EQ(cache.poll(), boost::none);
    EXPECT_EQ(cache.del(), boost::none);
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

    cache.put(1, 11);
    cache.put(2, 12);
    cache.put(3, 13);
    cache.put(4, 14);
    cache.put(5, 15);

    EXPECT_EQ(cache.get_size(), 5);

    cache.put(6, 600);
    cache.put(7, 700);
    cache.put(8, 800);
    cache.put(9, 900);
    cache.put(10, 1000);

    EXPECT_EQ(cache.get_size(), 5);
    cache.put(6, 600);

    kv_pair = *cache.poll();
    EXPECT_EQ(kv_pair.first, 6);
    EXPECT_EQ(kv_pair.second, 600);
}

}  // namespace
}  // namespace husky
