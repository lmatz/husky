#include "core/page.hpp"

#include <string>
#include <vector>

#include "gtest/gtest.h"


namespace husky {
namespace {

class TestPage : public testing::Test {
   public:
    TestPage() {}
    ~TestPage() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestPage, Functional) {
    Page p1(1, 0, 4194304);
    p1.swap_in_memory();
    EXPECT_EQ(p1.get_bin().size(), 0);
    EXPECT_EQ(p1.all_memory(), 4194304);

    std::vector<char> vec;
    for (char i = 'a'; i <= 'z'; i++) {
        vec.push_back(i);
    }

    BinStream bs(vec);
    EXPECT_EQ(p1.write(bs), 26);
    EXPECT_EQ(p1.write(vec), 26);

    BinStream& inner_bs = p1.get_bin();
    int size = inner_bs.size();
    EXPECT_EQ(size, 52);

    p1.write_to_disk();
    p1.clear_bin();
    EXPECT_FALSE(p1.check_bin_in_memory());
    size = inner_bs.size();
    EXPECT_EQ(size, 0);

    p1.read_from_disk();
    EXPECT_TRUE(p1.check_bin_in_memory());
    EXPECT_EQ(inner_bs.size(), 52);

    p1.write(std::move(vec));
    p1.write_to_disk();
    p1.clear_bin();
    p1.get_bin();
    EXPECT_TRUE(p1.check_bin_in_memory());
    EXPECT_EQ(inner_bs.size(), 78);

    p1.swap_out_memory();
    EXPECT_FALSE(p1.check_bin_in_memory());
    EXPECT_FALSE(p1.in_memory());
    p1.finalize();
}

}  // namespace
}  // namespace husky
