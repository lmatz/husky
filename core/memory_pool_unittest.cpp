#include "core/memory_pool.hpp"

#include <sstream>
#include <utility>

#include "gtest/gtest.h"

#include "core/objlist.hpp"
#include "core/objlist_data.hpp"
#include "core/objlist_store.hpp"

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
        Config config;
        config.set_param("maximum_thread_memory", "134217728");
        config.set_param("page_size", "2097152");
        Context::set_config(std::move(config));
        Context::set_local_tid(0);
    }
    void TearDown() {
        MemoryPool::free_mem_pool();
        PageStore::drop_all_pages();
        PageStore::free_page_map();
    }
};

class Obj {
   public:
    using KeyT = int;
    KeyT key;
    const KeyT& id() const { return key; }
    Obj() {}
    explicit Obj(const KeyT& k) : key(k) {}
};

TEST_F(TestMemoryPool, Functional) {
    std::stringstream str_stream(Context::get_param("page_size"));
    int64_t page_size;
    str_stream >> page_size;
    str_stream.clear();
    int64_t max_thread_mem;
    str_stream.str(Context::get_param("maximum_thread_memory"));
    str_stream >> max_thread_mem;

    auto& mem_pool = MemoryPool::get_mem_pool();

    size_t num_pages = max_thread_mem / page_size;
    EXPECT_EQ(mem_pool.capacity(), num_pages);
    EXPECT_EQ(mem_pool.num_pages_in_memory(), 0);

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
