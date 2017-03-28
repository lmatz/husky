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

#include "core/page_store.hpp"

#include <sstream>

#include "base/session_local.hpp"
#include "core/context.hpp"

namespace husky {

thread_local PageMap* PageStore::s_page_map_ = nullptr;
thread_local PageSet* PageStore::s_page_set_ = nullptr;
thread_local size_t PageStore::s_counter = 0;
// 4MB
const size_t PageStore::k_page_size_ = 4 * 1024 * 1024;

// set finalize_all_objlists priority to Level1, the higher the level, the higher the priority
static thread_local base::RegSessionThreadFinalizer finalize_all_objlists(base::SessionLocalPriority::Level1, []() {
    PageStore::drop_all_pages();
    PageStore::free_page_map();
});

void PageStore::drop_all_pages() {
    if (s_page_map_ == nullptr)
        return;
    for (auto& page_pair : (*s_page_map_)) {
        page_pair.second->finalize();
        delete page_pair.second;
    }
    s_page_map_->clear();
    s_page_set_->clear();
}

void PageStore::init_page_map() {
    if (s_page_map_ == nullptr)
        s_page_map_ = new PageMap();

    if (s_page_set_ == nullptr)
        s_page_set_ = new PageSet();
}

void PageStore::free_page_map() {
    if (s_page_map_ != nullptr)
        delete s_page_map_;
    s_page_map_ = nullptr;

    if (s_page_set_ != nullptr)
        delete s_page_set_;
    s_page_set_ = nullptr;

    s_counter = 0;
}

PageMap& PageStore::get_page_map() {
    init_page_map();
    return *s_page_map_;
}

}  // namespace husky
