// Copyright 2017 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>
#include <vector>

#include "core/engine.hpp"
#include "core/memory_checker.hpp"
#include "core/memory_pool.hpp"

using namespace husky;

class Obj {
   public:
    using KeyT = int;

    Obj() = default;
    explicit Obj(const KeyT& k) : key(k) {}
    const KeyT& id() const { return key; }

    int key;
    friend BinStream& operator<<(BinStream& stream, const Obj& o) {
        stream << o.key;
        return stream;
    }
    friend BinStream& operator>>(BinStream& stream, Obj& o) {
        stream >> o.key;
        return stream;
    }
};

void test_swapping() {
    auto& first_list = ObjListStore::create_objlist<Obj>();
    auto& second_list = ObjListStore::create_objlist<Obj>();
    auto& third_list = ObjListStore::create_objlist<Obj>();

    auto& mem_pool = MemoryPool::get_mem_pool();
    size_t max_thread_mem = mem_pool.max_thread_mem();
    BinStream obj_bs;
    Obj obj(1);
    obj_bs << obj;
    size_t obj_size = obj_bs.size() * sizeof(char);
    size_t max_obj_num = max_thread_mem / obj_size;
    size_t local_tid = Context::get_local_tid();

    if (local_tid == 0) {
        LOG_I << "max_thread_mem:" << max_thread_mem << " bytes";
        LOG_I << "obj_size:" << obj_size << " bytes";
        LOG_I << "max_obj_num:" << max_obj_num;
    }

    for (size_t i = 1; i <= max_obj_num / 2; i++) {
        Obj obj(i);
        first_list.add_object(obj);
    }
    LOG_I << "first_list size: " << first_list.byte_size_in_memory() << " bytes";

    auto& mem_checker = MemoryChecker::get_mem_checker();
    size_t mem_usage = mem_checker.memory_usage_by_objlist_on_thread(Context::get_local_tid());
    // since here our obj size is fixed, so the estimation is accurate
    LOG_I << "after adding objects to first_list";
    LOG_I << "cur_mem_usage:" << mem_usage << " bytes";
    LOG_I << "first_objlist:" << first_list.byte_size_in_memory() << " bytes";
    for (size_t i = 1; i <= max_obj_num - 1; i++) {
        Obj obj(i);
        second_list.add_object(obj);
    }

    LOG_I << "after add objects to second_list";
    mem_usage = mem_checker.memory_usage_by_objlist_on_thread(Context::get_local_tid());
    LOG_I << "cur_mem_usage:" << mem_usage << " bytes";
    LOG_I << "first_objlist:" << first_list.byte_size_in_memory() << " bytes";
    LOG_I << "second_objlist:" << second_list.byte_size_in_memory() << " bytes";

    for (size_t i = 1; i <= max_obj_num - 2; i++) {
        Obj obj(i);
        third_list.add_object(obj);
    }

    LOG_I << "after add objects to third_list";
    mem_usage = mem_checker.memory_usage_by_objlist_on_thread(Context::get_local_tid());
    LOG_I << "cur_mem_usage:" << mem_usage << " bytes";
    LOG_I << "first_objlist:" << first_list.byte_size_in_memory() << " bytes";
    LOG_I << "second_objlist:" << second_list.byte_size_in_memory() << " bytes";
    LOG_I << "third_objlist:" << third_list.byte_size_in_memory() << " bytes";

    mem_checker.serve();
    mem_checker.register_update_handler([&]() {
        std::vector<size_t> info = mem_checker.get_mem_info();
        LOG_I << "mem_usage on thread " << local_tid << " :" << info[local_tid];
    });
    ObjListStore::drop_objlist(third_list.get_id());
    std::this_thread::sleep_for(std::chrono::seconds(3));

    auto& fourth_list = ObjListStore::create_objlist<Obj>();
    for (size_t i = 1; i <= max_obj_num / 2; i++) {
        Obj obj(i);
        fourth_list.add_object(obj);
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    if (init_with_args(argc, argv, args)) {
        run_job(test_swapping);
        return 0;
    }
    return 1;
}
