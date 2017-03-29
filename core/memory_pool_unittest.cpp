#include "core/memory_pool.hpp"

#include <sstream>
#include <utility>

#include "gtest/gtest.h"
#include "core/page.hpp"
#include "core/page_store.hpp"

namespace husky {
namespace {

class TestMemoryPool : public testing::Test {
   public:
    TestMemoryPool() {}
    ~TestMemoryPool() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestMemoryPool, Functional) {
    size_t num_pages = 256;
    size_t page_size = 4*1024*1024;
    auto& mem_pool = MemoryPool::get_mem_pool();

    for (int i = 1; i <= num_pages; i++) {
        Page* p = PageStore::create_page();
        EXPECT_EQ(p->all_memory(), page_size);
        EXPECT_EQ(mem_pool.request_page(p->get_key(), p), true);
        EXPECT_EQ(mem_pool.num_pages_in_memory(), i);
    }

    for (int i = 1; i <= num_pages; i++) {
        Page* p = PageStore::create_page();
        EXPECT_EQ(p->all_memory(), page_size);
        EXPECT_EQ(mem_pool.request_page(p->get_key(), p), true);
        EXPECT_EQ(mem_pool.num_pages_in_memory(), num_pages);
    }

    EXPECT_EQ(mem_pool.request_space(1), page_size);
    EXPECT_EQ(mem_pool.num_pages_in_memory(), num_pages - 1);

    EXPECT_EQ(mem_pool.request_space(page_size), page_size);
    EXPECT_EQ(mem_pool.num_pages_in_memory(), num_pages - 2);

    EXPECT_EQ(mem_pool.request_space(page_size + 1), page_size * 2);
    EXPECT_EQ(mem_pool.num_pages_in_memory(), num_pages - 4);

    for (int i = 5; i >= 0; i--) {
        EXPECT_EQ(mem_pool.request_space(page_size - 1), page_size);
        EXPECT_EQ(mem_pool.num_pages_in_memory(), num_pages + i - 10);
    }

    EXPECT_EQ(mem_pool.request_space(1), page_size);
    EXPECT_EQ(mem_pool.num_pages_in_memory(), num_pages - 11 < 0 ? 0 : num_pages - 11);
    EXPECT_EQ(mem_pool.capacity(), num_pages);
}

}  // namespace
}  // namespace husky
