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

#include "core/memory_checker.hpp"

#include <mutex>

#include "core/objlist.hpp"
#include "core/objlist_store.hpp"

namespace husky {

MemoryChecker& MemoryChecker::get_mem_checker() {
    static MemoryChecker mem_checker;
    return mem_checker;
}

void MemoryChecker::add_objlist_on_thread(size_t local_tid, ObjListBase* objlist) {
    std::lock_guard<std::mutex> lock(mu_);
    if (local_tid >= objlists_on_thread_.size())
        throw base::HuskyException("MemoryChecker::add_objlist_on_thread error: tid out of range");
    objlists_on_thread_[local_tid].insert(objlist);
}

void MemoryChecker::delete_objlist_on_thread(size_t local_tid, ObjListBase* objlist) {
    std::lock_guard<std::mutex> lock(mu_);
    if (local_tid >= objlists_on_thread_.size())
        throw base::HuskyException("MemoryChecker::delete_objlist_on_thread error: tid out of range");
    objlists_on_thread_[local_tid].erase(objlist);
}

size_t MemoryChecker::memory_usage_by_objlist_on_thread(size_t local_tid) {
    std::lock_guard<std::mutex> lock(mu_);
    if (local_tid >= objlists_on_thread_.size())
        throw base::HuskyException("MemoryChecker::memory_usage_by_objlist_on_thread error: tid out of range");
    auto& objlist_set = objlists_on_thread_[local_tid];
    size_t all_bytes = 0;
    for (auto it = objlist_set.begin(); it != objlist_set.end(); ++it) {
        ObjListBase* objlist = *it;
        all_bytes += objlist->byte_size_in_memory();
    }
    return all_bytes;
}

void MemoryChecker::serve() { checker_ = std::thread(&MemoryChecker::loop, this); }

void MemoryChecker::stop() {
    stop_thread_ = true;
    if (checker_.joinable())
        checker_.join();
}

void MemoryChecker::register_update_handler(const std::function<void()>& handler) { update_handler_ = handler; }

void MemoryChecker::loop() {
    while (!stop_thread_) {
        update();

        if (update_handler_ != nullptr)
            update_handler_();
        std::this_thread::sleep_for(sleep_duration_);
    }
}

void MemoryChecker::update() {
    size_t num_workers = Context::get_num_local_workers();
    for (size_t i = 0; i < num_workers; i++) {
        size_t mem_usage = memory_usage_by_objlist_on_thread(i);
        memory_usage_on_thread_[i] = mem_usage;
    }
}

}  // namespace husky
