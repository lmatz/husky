#include "core/memory_pool.hpp"

#include <sstream>
#include <utility>

#include "core/page.hpp"
#include "core/page_store.hpp"
#include "gtest/gtest.h"

namespace husky {
namespace {

class TestMemoryPool : public testing::Test {
   public:
    TestMemoryPool() {}
    ~TestMemoryPool() {}

   protected:
    void SetUp() {
        MemoryPool::free_mem_pool();
        PageStore::drop_all_pages();
        PageStore::free_page_map();
    }
    void TearDown() {
        MemoryPool::free_mem_pool();
        PageStore::drop_all_pages();
        PageStore::free_page_map();
    }
};

TEST_F(TestMemoryPool, Functional) {
    size_t num_pages = MemoryPool::get_mem_pool().capacity();
    size_t page_size = 4 * 1024 * 1024;
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

    size_t bytes_evicted = 0;
    if (mem_pool.num_pages_in_memory() > 0) {
        bytes_evicted = page_size;
    }
    EXPECT_EQ(mem_pool.request_space(1), bytes_evicted);
    EXPECT_EQ(mem_pool.num_pages_in_memory(), num_pages - 1);

    if (mem_pool.num_pages_in_memory() > 0) {
        bytes_evicted = page_size;
    }
    else {
        bytes_evicted = 0;
    }
    EXPECT_EQ(mem_pool.request_space(page_size), bytes_evicted);
    EXPECT_EQ(mem_pool.num_pages_in_memory(), (num_pages < 2)?0:(num_pages-2));

    if (mem_pool.num_pages_in_memory() > 1) {
        bytes_evicted = 2*page_size;
    }
    else if (mem_pool.num_pages_in_memory() == 1) {
        bytes_evicted = page_size;
    }
    else {
        bytes_evicted = 0;
    }
    EXPECT_EQ(mem_pool.request_space(page_size + 1), bytes_evicted);
    EXPECT_EQ(mem_pool.num_pages_in_memory(), (num_pages < 4)?0:(num_pages-4));

    for (int i = 5; i >= 0; i--) {
        if (mem_pool.num_pages_in_memory() > 0) {
            bytes_evicted = page_size;
        }
        else {
            bytes_evicted = 0;
        }
        EXPECT_EQ(mem_pool.request_space(page_size - 1), bytes_evicted);
        EXPECT_EQ(mem_pool.num_pages_in_memory(), (num_pages < (10-i))?0:(num_pages + i - 10));
    }

    if (mem_pool.num_pages_in_memory() > 0) {
        bytes_evicted = page_size;
    }
    else {
        bytes_evicted = 0;
    }
    EXPECT_EQ(mem_pool.request_space(1), bytes_evicted);
    EXPECT_EQ(mem_pool.num_pages_in_memory(), (num_pages < 11) ? 0 : (num_pages - 11));
    EXPECT_EQ(mem_pool.capacity(), num_pages);
}

}  // namespace
}  // namespace husky
