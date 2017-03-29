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

#include "core/memory_pool.hpp"

#include <sstream>
#include <string>

#include "base/log.hpp"
#include "core/page.hpp"
#include "core/page_store.hpp"

namespace husky {

thread_local MemoryPool* MemoryPool::mem_pool_ = nullptr;

MemoryPool& MemoryPool::get_mem_pool() {
    init_mem_pool();
    return *mem_pool_;
}

void MemoryPool::init_mem_pool() {
    if (mem_pool_ == nullptr) {
        mem_pool_ = new MemoryPool();
    }
}

void MemoryPool::free_mem_pool() {
    if (mem_pool_ != nullptr)
        delete mem_pool_;
    mem_pool_ = nullptr;
}

MemoryPool::MemoryPool() {
    size_t max_thread_mem = 1024*1024*1024;
    size_t page_size = PageStore::k_page_size;
    num_pages_ = max_thread_mem / page_size;
    DLOG_I << "MemoryPool num_pages:" << num_pages_;
    cache_ = new LRUCache<typename Page::KeyT, Page*>(num_pages_);
}

// true indicates that objlist with 'key' is brought to memory from disk
// false indicates that objlist with 'key' has already existsed in memory
bool MemoryPool::request_page(const typename Page::KeyT& key, Page* page) {
    bool live_in_memory = cache_->exists(key);
    // first check whether the page we request currently lives in the memory
    // if yes, then nothing we need to do, just return
    if (live_in_memory)
        return false;
    // if not, then check out the candidate that may be deleted by the cache in the 'put' operation
    auto first_delete_candidate = cache_->poll();
    cache_->put(key, page);
    // check out the candiidate to be deleted again
    auto second_delete_candidate = cache_->poll();
    // if they are not the same, which means the first candidate just get deleted
    if (first_delete_candidate != boost::none && first_delete_candidate != second_delete_candidate) {
        (*first_delete_candidate).second->swap_out_memory();
    }
    // first swap out the old page, then bring in the new page
    page->swap_in_memory();
    return true;
}

// in case that user want to get some free space without swapping in a page
size_t MemoryPool::request_space(const size_t bytes_required) {
    size_t bytes_evicted = 0;
    int count = 0;
    // check out the number of pages in cache
    int num = cache_->get_size();
    while (bytes_evicted < bytes_required && count < num) {
        auto del_candidate = cache_->poll();
        typename Page::KeyT key = (*del_candidate).first;
        Page* page = (*del_candidate).second;
        // currently we use all memory a page can hold to determine the memory used by this page
        size_t page_bytes = page->all_memory();
        page->swap_out_memory();
        bytes_evicted += page_bytes;
        cache_->del();
        count++;
    }
    return bytes_evicted;
}
}  // namespace husky
