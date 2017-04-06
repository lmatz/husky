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

#pragma once

#include <stddef.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "core/context.hpp"
#include "core/objlist.hpp"

namespace husky {

class MemoryChecker {
   public:
    static MemoryChecker& get_mem_checker();

    void add_objlist_on_thread(size_t tid, ObjListBase* objlist);
    void delete_objlist_on_thread(size_t tid, ObjListBase* objlist);

    void serve();
    void stop();
    void register_update_handler(const std::function<void()>& handler);

    size_t memory_usage_by_objlist_on_thread(size_t local_tid);

    inline std::vector<size_t>& get_mem_info() { return memory_usage_on_thread_; }

   protected:
    void loop();
    void update();

   private:
    explicit MemoryChecker(int seconds = 1)
        : sleep_duration_(seconds), stop_thread_(false), checker_(), update_handler_(nullptr) {
        objlists_on_thread_.resize(Context::get_num_local_workers());
        memory_usage_on_thread_.resize(Context::get_num_local_workers());
    }

    ~MemoryChecker() {
        if (!stop_thread_)
            stop();
    }

    std::mutex mu_;
    std::chrono::seconds sleep_duration_;
    std::atomic_bool stop_thread_;
    std::thread checker_;
    std::function<void()> update_handler_;
    std::vector<std::unordered_set<ObjListBase*>> objlists_on_thread_;
    std::vector<size_t> memory_usage_on_thread_;
};
}  // namespace husky
