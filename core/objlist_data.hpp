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

#include <vector>

#include "base/exception.hpp"
#include "base/serialization.hpp"
#include "core/memory_pool.hpp"
#include "core/page.hpp"
#include "core/page_store.hpp"

namespace husky {

using base::BinStream;

template <typename ObjT>
class ObjListData {
   public:
    ObjListData() = default;
    ~ObjListData() {
        for (size_t i = 0; i < pages_.size(); i++) {
            PageStore::release_page(pages_[i]);
        }
    }

    ObjListData(const ObjListData&) = delete;
    ObjListData& operator=(const ObjListData&) = delete;

    ObjListData(ObjListData&&) = delete;
    ObjListData& operator=(ObjListData&&) = delete;

    inline size_t get_size() const {
        if (check_data_in_memory()) {
            return data_.size() - num_del_;
        }
        return data_size_ - num_del_;
    }

    inline size_t get_vector_size() const {
        if (check_data_in_memory()) {
            return data_.size();
        }
        return data_size_;
    }

    void clear() { data_.clear(); }

    inline std::vector<ObjT>& get_data() {
        if (!check_data_in_memory()) {
            read_data_from_disk();
        }
        return data_;
    }

    ObjT& get(size_t i) {
        get_data();
        if (i < 0 || i >= data_.size()) {
            throw base::HuskyException("ObjListData<T>::get error: index out of range");
        }
        return data_[i];
    }

    // Find the index of an obj
    size_t index_of(const ObjT* const obj_ptr) const {
        const_cast<ObjListData<ObjT>*>(this)->get_data();
        size_t idx = obj_ptr - &data_[0];
        if (idx < 0 || idx >= data_.size())
            throw base::HuskyException("ObjListData<T>::index_of error: index out of range");
        return idx;
    }

    friend BinStream& operator<<(BinStream& stream, ObjListData<ObjT>& obj_list_data) {
        return stream << obj_list_data.data_;
    }

    friend BinStream& operator>>(BinStream& stream, ObjListData<ObjT>& obj_list_data) {
        return stream >> obj_list_data.data_;
    }

    bool check_data_in_memory() const { return data_in_memory_; }

    bool check_pages_in_memory() const {
        for (size_t i = 0; i < pages_.size(); i++) {
            if (!pages_[i]->in_memory()) {
                return false;
            }
        }
        return true;
    }

    void write_to_disk() {
        // serialize the data into binstream
        BinStream bs;
        bs << data_;
        byte_size_ = bs.size();

        // since deletion finalize may actually delete some data
        // we may not need that many pages any more, so get rid of some pages
        size_t num_pages = pages_.size();
        while (byte_size_ <= (page_size_ * (num_pages - 1)) && num_pages >= 1) {
            Page* page = pages_.back();
            pages_.pop_back();
            PageStore::release_page(page);
            num_pages -= 1;
        }

        // make sure every page is in the memory
        read_pages_from_disk();
        size_t start = 0;
        DLOG_I << "the size of binstream written to disk is:" << bs.size();
        for (int i = 0; i < pages_.size(); i++) {
            pages_[i]->clear_bin();
            // write a subset of binstream to this page
            BinStream sub = bs.sub_stream(start, page_size_);
            size_t written = sub.size();
            pages_[i]->write(std::move(sub));
            bool ret = pages_[i]->write_to_disk();
            if (!ret)
                throw base::HuskyException("when writing data to disk, a page cannot write to its file.");
            start += written;
        }
        // all data already on disk, so clear it from memory
        clear_data_from_memory();
    }

    void clear_data_from_memory() {
        data_size_ = data_.size();
        std::vector<ObjT> tmp;
        data_.swap(tmp);
        byte_size_ = 0;
        data_in_memory_ = false;
    }

    // put all pages into memory pool
    void read_pages_from_disk() {
        if (check_pages_in_memory()) {
            return;
        }
        // if objlist_data has more pages than the memory pool can hold, throw expcetion
        if (pages_.size() > MemoryPool::get_mem_pool().capacity()) {
            throw base::HuskyException(
                "read pages from disk: The memory pool cannot hold all pages in this objlist_data");
        }
        // if not all pages are in memory, try to bring them into memory
        for (int i = 0; i < pages_.size(); i++) {
            if (!pages_[i]->in_memory()) {
                MemoryPool::get_mem_pool().request_page(pages_[i]->get_key(), pages_[i]);
            }
        }
    }

    // put all pages into memory pool and turn them into a data vector
    void read_data_from_disk() {
        read_pages_from_disk();
        BinStream bs;
        for (int i = 0; i < pages_.size(); i++) {
            bs.append(pages_[i]->get_bin());
            pages_[i]->clear_bin();
        }
        bs >> data_;
        data_size_ = data_.size();
        byte_size_ = bs.size();
        data_in_memory_ = true;
    }

   private:
    std::vector<ObjT> data_;
    std::vector<Page*> pages_;
    bool data_in_memory_ = true;
    size_t num_del_ = 0;
    size_t byte_size_ = 0;
    size_t data_size_ = 0;
    const size_t page_size_ = PageStore::k_page_size;

    template <typename T>
    friend class ObjList;
};

}  // namespace husky
