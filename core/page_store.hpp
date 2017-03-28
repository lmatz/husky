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

#include <unordered_map>
#include <unordered_set>

#include "base/log.hpp"
#include "core/page.hpp"

namespace husky {

typedef std::unordered_map<size_t, Page*> PageMap;

typedef std::unordered_set<Page*> PageSet;

class PageStore {
   public:
    static Page* create_page() {
        PageMap& page_map = get_page_map();
        Page* page;
        if (s_page_set_->size() != 0) {
            auto it = s_page_set_->begin();
            page = *it;
            s_page_set_->erase(it);
            return page;
        }
        page = new Page(s_counter, k_page_size_);
        s_counter++;
        page_map.insert({page->get_key(), page});
        DLOG_I << "page store has " << page_map.size() << " pages";
        return page;
    }

    static bool release_page(Page* page) {
        PageMap& page_map = get_page_map();
        if (page_map.find(page->get_key()) == page_map.end())
            throw base::HuskyException("The page to be released is not created by this page store");
        if (s_page_set_->find(page) != s_page_set_->end())
            return false;
        page->set_owner(nullptr);
        s_page_set_->insert(page);
        return true;
    }

    static void drop_all_pages();

    static void free_page_map();

   protected:
    static void init_page_map();
    static PageMap& get_page_map();

   protected:
    static thread_local PageSet* s_page_set_;
    static thread_local PageMap* s_page_map_;
    static thread_local size_t s_counter;
    static const size_t k_page_size_;
};

}  // namespace husky
