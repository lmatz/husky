#include "core/page.hpp"

#include <algorithm>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "core/memory_pool.hpp"
#include "core/page_store.hpp"

namespace husky {
namespace {

class TestPage : public testing::Test {
   public:
    TestPage() {}
    ~TestPage() {}

   protected:
    void SetUp() {
        MemoryPool::free_mem_pool();
        PageStore::drop_all_pages();
        PageStore::free_page_map();
        Config config;
        int64_t max_thread_mem = 1024 * 1024 * 32;
        config.set_param("maximum_thread_memory", std::to_string(max_thread_mem));
        config.set_param("page_size", "4194304");
        Context::set_config(std::move(config));
        Context::set_local_tid(0);
    }
    void TearDown() {
        MemoryPool::free_mem_pool();
        PageStore::drop_all_pages();
        PageStore::free_page_map();
    }
};

TEST_F(TestPage, Functional) {
    Context::set_local_tid(0);
    Page* p1 = PageStore::create_page();
    Page* p2 = PageStore::create_page();

    EXPECT_TRUE(MemoryPool::get_mem_pool().request_page(p1->get_key(), p1));

    std::vector<char> vec;
    for (int i = 0; i < 26; i++) {
        vec.push_back('a' + i);
    }

    BinStream bs(vec);
    EXPECT_EQ(p1->write(bs), 26);
    EXPECT_EQ(p1->write(vec), 26);

    BinStream& inner_bs = p1->get_bin();
    int size = inner_bs.size();
    EXPECT_EQ(size, 52);

    p1->clear_bin();
    size = inner_bs.size();
    EXPECT_EQ(size, 0);
}

}  // namespace
}  // namespace husky
