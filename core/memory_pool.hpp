// Copyright 2016 Husky Team
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

#include <list>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "core/cache.hpp"
#include "core/page.hpp"

namespace husky {

class MemoryPool {
   public:
    static MemoryPool& get_mem_pool();

    static void init_mem_pool();
    static void free_mem_pool();

    // we bring the objlist with 'key' into the momery
    // ObjListPool is not aware of any memory issue
    // It will do the job when caller think it is approriate
    bool request_page(const typename Page::KeyT& key, Page* page);

    // bring the objlist chosen according to the cache policy from memory onto disk
    // by default, we will evict one objlist
    // or the caller has a better idea of how many bytes it needs exactly
    int64_t request_space(const int64_t bytes_required = 1);

    inline bool in_memory(const typename Page::KeyT& key) { return cache_->exists(key); }

    inline bool on_disk(const typename Page::KeyT&& key) { return !cache_->exists(key); }

    inline size_t num_pages_in_memory() { return cache_->get_size(); }

    // the maximum number of pages allowed by the pool
    inline size_t capacity() { return num_pages_; }

   private:
    MemoryPool();

    LRUCache<typename Page::KeyT, Page*>* cache_;
    int64_t num_pages_;

    static thread_local MemoryPool* mem_pool_;
};

}  // namespace husky
