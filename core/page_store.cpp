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

#include "core/page_store.hpp"

#include "base/session_local.hpp"

namespace husky {

thread_local PageMap* PageStore::page_map_ = nullptr;
thread_local PageSet* PageStore::page_set_ = nullptr;
thread_local size_t PageStore::s_counter = 0;

// set finalize_all_objlists priority to Level1, the higher the level, the higher the priority
static thread_local base::RegSessionThreadFinalizer finalize_all_objlists(base::SessionLocalPriority::Level1, []() {
    PageStore::drop_all_pages();
    PageStore::free_page_map();
});

void PageStore::drop_all_pages() {
    if (page_map_ == nullptr)
        return;
    for (auto& page_pair : (*page_map_)) {
        page_pair.second->finalize();
        delete page_pair.second;
    }
    page_map_->clear();
    page_set_->clear();
}

void PageStore::init_page_map() {
    if (page_map_ == nullptr)
        page_map_ = new PageMap();

    if (page_set_ == nullptr)
        page_set_ = new PageSet();
}

void PageStore::free_page_map() {
    if (page_map_ != nullptr)
        delete page_map_;
    page_map_ = nullptr;

    if (page_set_ != nullptr)
        delete page_set_;
    page_set_ = nullptr;

    s_counter = 0;
}

PageMap& PageStore::get_page_map() {
    init_page_map();
    return *page_map_;
}

}  // namespace husky
