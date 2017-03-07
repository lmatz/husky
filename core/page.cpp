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

#include "page.hpp"

#include <string>

#include "objlist.hpp"

namespace husky {

void Page::swap_in_memory() {
    // set the flag first so that other method can be called
    in_memory_ = true;
    read_from_disk();
}

void Page::swap_out_memory() {
    // tell the objlist data that this page owned by it is swapped out of memory.
    DLOG_I << "page " + std::to_string(this->id_) + " is about to be swapped out of the memory";
    if (owner_ != nullptr) {
        DLOG_I << "owner is not nullptr";
        owner_->clear_page_from_memory(this);
    }
    bool ret = write_to_disk();
    if (!ret)
        throw base::HuskyException("page " + std::to_string(this->id_) +
                                   " fails to write to disk when being swapped out of the memory");
    clear_bin();
    in_memory_ = false;
}

void Page::set_owner(ObjListBase* owner) { owner_ = owner; }

ObjListBase* Page::get_owner() { return owner_; }

}  // namespace husky
