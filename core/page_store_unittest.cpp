#include "core/page_store.hpp"

#include <string>

#include "core/page.hpp"
#include "gtest/gtest.h"

namespace husky {
namespace {

class TestPageStore : public testing::Test {
   public:
    TestPageStore() {}
    ~TestPageStore() {}

   protected:
    void SetUp() {
        PageStore::drop_all_pages();
        PageStore::free_page_map();
    }
    void TearDown() {
        PageStore::drop_all_pages();
        PageStore::free_page_map();
    }
};

TEST_F(TestPageStore, Functional) {
    Page* p1 = PageStore::create_page();
    EXPECT_EQ(p1->get_key(), 0);
    auto p2 = PageStore::create_page();
    EXPECT_EQ(p2->get_key(), 1);

    EXPECT_TRUE(PageStore::release_page(p1));
    EXPECT_FALSE(PageStore::release_page(p1));

    auto p3 = PageStore::create_page();
    EXPECT_EQ(p3->get_key(), 0);
    EXPECT_EQ(p3, p1);
    auto p4 = PageStore::create_page();
    EXPECT_EQ(p4->get_key(), 2);
}

}  // namespace
}  // namespace husky
