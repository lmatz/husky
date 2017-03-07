#include "core/objlist.hpp"

#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "core/memory_pool.hpp"
#include "core/page_store.hpp"

namespace husky {
namespace {

class TestObjList : public testing::Test {
   public:
    TestObjList() {}
    ~TestObjList() {}

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

class Obj {
   public:
    using KeyT = int64_t;
    KeyT key;
    const KeyT& id() const { return key; }
    Obj() {}
    explicit Obj(const KeyT& k) : key(k) {}

    friend BinStream& operator<<(BinStream& stream, Obj& obj) { return stream << obj.key; }

    friend BinStream& operator>>(BinStream& stream, Obj& obj) { return stream >> obj.key; }
};

size_t size__ = 100;

TEST_F(TestObjList, InitAndDelete) {
    ObjList<Obj>* obj_list = new ObjList<Obj>();
    ASSERT_TRUE(obj_list != nullptr);
    delete obj_list;
}

TEST_F(TestObjList, AddObject) {
    ObjList<Obj> obj_list;
    for (int64_t i = 0; i < size__; ++i) {
        Obj obj(i);
        obj_list.add_object(obj);
    }
    std::vector<Obj>& v = obj_list.get_data();
    for (int64_t i = 0; i < size__; ++i) {
        EXPECT_EQ(v[i].key, i);
    }
}

TEST_F(TestObjList, AddMoveObject) {
    ObjList<Obj> obj_list;
    for (int i = 0; i < size__; ++i) {
        Obj obj(i);
        obj_list.add_object(std::move(obj));
    }
    std::vector<Obj>& v = obj_list.get_data();
    for (int i = 0; i < size__; ++i) {
        EXPECT_EQ(v[i].key, i);
    }
}

TEST_F(TestObjList, Sort) {
    ObjList<Obj> obj_list;
    for (int i = 0; i < size__; ++i) {
        Obj obj(size__ - i - 1);
        obj_list.add_object(std::move(obj));
    }
    obj_list.sort();
    EXPECT_EQ(obj_list.get_sorted_size(), size__);
    EXPECT_EQ(obj_list.get_num_del(), 0);
    EXPECT_EQ(obj_list.get_hashed_size(), 0);
    EXPECT_EQ(obj_list.get_size(), size__);
    std::vector<Obj>& v = obj_list.get_data();
    for (int i = 0; i < size__; ++i) {
        EXPECT_EQ(v[i].key, i);
    }
}

TEST_F(TestObjList, Delete) {
    ObjList<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        obj_list.add_object(std::move(obj));
    }
    EXPECT_EQ(obj_list.get_size(), 10);
    std::vector<Obj>& v = obj_list.get_data();
    Obj* p = &v[3];
    Obj* p2 = &v[7];
    obj_list.delete_object(p);
    EXPECT_EQ(obj_list.get_num_del(), 1);
    EXPECT_EQ(obj_list.get_size(), 9);
    obj_list.delete_object(p2);
    EXPECT_EQ(obj_list.get_size(), 8);
    EXPECT_EQ(obj_list.get_num_del(), 2);
    EXPECT_EQ(obj_list.get_del(3), 1);
    EXPECT_EQ(obj_list.get_del(5), 0);
    obj_list.deletion_finalize();
    EXPECT_EQ(obj_list.get_num_del(), 0);
    EXPECT_EQ(obj_list.get_size(), 8);
}

TEST_F(TestObjList, Find) {
    ObjList<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        obj_list.add_object(std::move(obj));
    }
    EXPECT_NE(obj_list.find(3), nullptr);
    EXPECT_NE(obj_list.find(5), nullptr);
    EXPECT_EQ(obj_list.find(10), nullptr);
    obj_list.sort();
    EXPECT_NE(obj_list.find(3), nullptr);
    EXPECT_NE(obj_list.find(5), nullptr);
    EXPECT_EQ(obj_list.find(10), nullptr);
}

TEST_F(TestObjList, IndexOf) {
    ObjList<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        obj_list.add_object(std::move(obj));
    }
    Obj* p = &obj_list.get_data()[2];
    Obj* p2 = &obj_list.get_data()[6];
    EXPECT_EQ(obj_list.index_of(p), 2);
    EXPECT_EQ(obj_list.index_of(p2), 6);
    Obj o;
    Obj* p3 = &o;
    try {
        obj_list.index_of(p3);
    } catch (...) {
    }
}

TEST_F(TestObjList, EstimatedStorage) {
    std::string str = Context::get_param("maximum_thread_memory");
    std::stringstream str_stream(str);
    int64_t max_thread_mem;
    str_stream >> max_thread_mem;
    int size_obj = sizeof(Obj);

    const size_t len = max_thread_mem / (size_obj + 4);
    ObjList<Obj> test_list;
    for (size_t i = 0; i < len; ++i) {
        Obj obj(i);
        test_list.add_object(std::move(obj));
    }

    const double sample_rate = 0.005;
    size_t estimated_storage = test_list.estimated_storage_size(sample_rate);
    size_t real_storage = test_list.get_data().capacity() * sizeof(Obj);  // just in this class Obj case
    double diff_time =
        double(std::max(real_storage, estimated_storage)) / double(std::min(real_storage, estimated_storage));
    EXPECT_EQ(diff_time, 1);
}

TEST_F(TestObjList, Large) {
    auto& mem_pool = MemoryPool::get_mem_pool();

    std::string str = Context::get_param("maximum_thread_memory");
    std::stringstream str_stream(str);
    int64_t max_thread_mem;
    str_stream >> max_thread_mem;
    int size_obj = sizeof(Obj);
    str = Context::get_param("page_size");
    str_stream.clear();
    str_stream.str(str);
    int page_size;
    str_stream >> page_size;

    EXPECT_EQ(mem_pool.capacity(), max_thread_mem / page_size);

    const size_t len = max_thread_mem / size_obj;
    const size_t del_len = 1024 * 1024;
    ObjList<Obj> list1;
    ObjList<Obj> list2;
    ObjList<Obj> list3;

    for (size_t i = 0; i < len; ++i) {
        Obj obj(i);
        list1.add_object(obj);
    }

    for (size_t i = 0; i < del_len; ++i) {
        list1.delete_object(i);
    }

    for (size_t i = 0; i < len; ++i) {
        Obj obj(i);
        list2.add_object(obj);
    }

    EXPECT_FALSE(list1.check_data_in_memory());
    EXPECT_EQ(list1.get_size(), len - del_len);

    Obj* ptr = list1.find(1024 * 1024);
    size_t idx = list1.index_of(ptr);
    EXPECT_EQ(idx, 0);

    EXPECT_FALSE(list2.check_data_in_memory());
    EXPECT_EQ(list1.get_size(), len - del_len);

    list1.delete_object(1024 * 1024);
    EXPECT_EQ(list1.get_num_del(), 1);
    list1.deletion_finalize();
    EXPECT_EQ(list1.get_num_del(), 0);
    EXPECT_EQ(list1.get_size(), len - del_len - 1);
}

}  // namespace
}  // namespace husky
