#include "core/page_store.hpp"

#include <string>

#include "gtest/gtest.h"

#include "base/session_local.hpp"
#include "core/memory_pool.hpp"

namespace husky {
namespace {

class TestPageStore : public testing::Test {
   public:
    TestPageStore() {}
    ~TestPageStore() {}

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

TEST_F(TestPageStore, Functional) {
    Page* p1 = PageStore::create_page();
    EXPECT_EQ(p1->get_id(), 0);
    auto p2 = PageStore::create_page();
    EXPECT_EQ(p2->get_id(), 1);
    auto p3 = PageStore::create_page();
    EXPECT_EQ(p3->get_id(), 2);
    auto p4 = PageStore::create_page();
    EXPECT_EQ(p4->get_id(), 3);
}

}  // namespace
}  // namespace husky
