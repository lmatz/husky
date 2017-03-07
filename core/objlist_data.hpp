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

#include <stdint.h>
#include <sstream>
#include <string>
#include <vector>

#include "base/exception.hpp"
#include "base/serialization.hpp"
#include "core/memory_pool.hpp"
#include "core/page.hpp"
#include "core/page_store.hpp"

namespace husky {

using base::BinStream;

class ObjListDataBase {};

template <typename ObjT>
class ObjListData : public ObjListDataBase {
   public:
    ObjListData() {
        std::string page_size = Context::get_param("page_size");
        std::stringstream str_stream;
        str_stream.str(page_size);
        str_stream >> page_size_;
        DLOG_I << "ObjListData constructor page size:" << page_size_;
    }

    ~ObjListData() {
        for (int i = 0; i < pages_.size(); i++) {
            PageStore::release_page(pages_[i]);
        }
    }

    ObjListData(const ObjListData&) = delete;
    ObjListData& operator=(const ObjListData&) = delete;

    ObjListData(ObjListData&&) = delete;
    ObjListData& operator=(ObjListData&&) = delete;

    inline size_t get_sorted_size() const { return sorted_size_; }

    inline size_t get_num_del() const { return num_del_; }

    inline size_t get_hashed_size() const { return hashed_objs_.size(); }

    inline size_t get_size() const {
        if (!check_data_in_memory()) {
            return size_ - num_del_;
        }
        return data_.size() - num_del_;
    }

    inline size_t get_vector_size() const {
        if (!check_data_in_memory()) {
            return size_;
        }
        return data_.size();
    }

    inline ObjT& get(size_t idx) {
        if (!check_data_in_memory()) {
            read_data_from_disk();
        }
        return data_[idx];
    }

    inline void clear() { data_.clear(); }

    inline std::vector<ObjT>& get_data() const {
        if (!check_data_in_memory()) {
            read_data_from_disk();
        }
        return data_;
    }

    inline std::vector<bool>& get_del_bitmap() const { return del_bitmap_; }

    inline std::unordered_map<typename ObjT::KeyT, size_t>& get_hashed_objs() const { return hashed_objs_; }

    inline size_t& get_sorted_size() { return sorted_size_; }

    inline size_t& get_num_del() { return num_del_; }

    size_t delete_object(const ObjT* const obj_ptr) {
        size_t idx = index_of(obj_ptr);
        if (del_bitmap_[idx] == false) {
            del_bitmap_[idx] = true;
            num_del_ += 1;
        }
        return idx;
    }

    size_t delete_object(const typename ObjT::KeyT& key) {
        size_t idx;
        if (hashed_objs_.find(key) != hashed_objs_.end()) {
            idx = hashed_objs_[key];
        } else {
            idx = index_of(key);
        }
        if (del_bitmap_[idx] == false) {
            del_bitmap_[idx] = true;
            num_del_ += 1;
        }
        return idx;
    }

    // Find the index of an obj
    size_t index_of(const typename ObjT::KeyT& key) const {
        if (!check_data_in_memory()) {
            read_data_from_disk();
        }
        for (int i = 0; i < data_.size(); i++) {
            if (data_[i].id() == key) {
                return i;
            }
        }
        throw base::HuskyException("ObjListData<T>::index_of error: key out of range");
    }

    size_t index_of(const ObjT* const obj_ptr) const {
        if (!check_data_in_memory()) {
            throw base::HuskyException("ObjListData<T>::index_of error: data is not in memory");
            read_data_from_disk();
        }
        size_t idx = obj_ptr - &data_[0];
        if (idx < 0 || idx >= data_.size())
            throw base::HuskyException("ObjListData<T>::index_of error: index out of range");
        return idx;
    }

    friend BinStream& operator<<(BinStream& stream, ObjListData<ObjT>& objlist_data) {
        return stream << objlist_data.data_;
    }

    friend BinStream& operator>>(BinStream& stream, ObjListData<ObjT>& objlist_data) {
        return stream >> objlist_data.data_;
    }

    // check whether data_ is valid to be manipulated
    bool check_data_in_memory() const { return data_in_memory_; }

    // check whether all the pages are in the memory
    bool check_pages_in_memory() const {
        for (int i = 0; i < pages_.size(); i++) {
            if (!pages_[i]->in_memory()) {
                return false;
            }
        }
        return true;
    }

   protected:
    // write data to the pages
    void write_to_disk() {
        // serialize the data into binstream
        BinStream bs;
        bs << data_;
        byte_size_ = bs.size();

        // since deletion finalize may actually delete some data
        // we may not need that many pages any more
        // get rid of some
        size_t num_pages = pages_.size();
        DLOG_I << "before we have " << num_pages << " pages";
        while (byte_size_ + page_size_ <= page_size_ * num_pages && num_pages >= 1) {
            Page* page = pages_.back();
            pages_.pop_back();
            PageStore::release_page(page);
            num_pages -= 1;
        }
        DLOG_I << "after we have " << num_pages << " pages";

        // make sure every page is in the memory
        read_pages_from_disk();
        size_t start = 0;
        size_t size_bin = bs.size();
        DLOG_I << "the size of binstream written to disk is:" << size_bin;
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

    // turn data_ into a emptry vector
    void clear_data_from_memory() {
        size_ = data_.size();
        std::vector<ObjT> tmp;
        data_.swap(tmp);
        byte_size_ = 0;
        data_in_memory_ = false;
    }

    // put all pages into memory pool
    void read_pages_from_disk() const {
        DLOG_I << "read pages from disk";
        if (check_pages_in_memory()) {
            DLOG_I << "all pages are in memory";
            return;
        }
        DLOG_I << "Not all pages are in memory";
        for (int i = 0; i < pages_.size(); i++) {
            if (!pages_[i]->in_memory()) {
                MemoryPool::get_mem_pool().request_page(pages_[i]->get_key(), pages_[i]);
            }
        }
        if (!check_pages_in_memory())
            throw base::HuskyException(
                "read pages from disk: The memory pool cannot hold all pages in this objlist_data");
    }
    // put all pages into memory pool and turn them into a data vector
    void read_data_from_disk() const {
        BinStream bs;
        read_pages_from_disk();
        for (int i = 0; i < pages_.size(); i++) {
            bs.append(pages_[i]->get_bin());
        }
        bs >> data_;
        byte_size_ = data_.size();
        data_in_memory_ = true;
        for (int i = 0; i < pages_.size(); i++) {
            pages_[i]->clear_bin();
        }
    }

   private:
    // info about all the pages which required by this objlist_data
    mutable std::vector<Page*> pages_;
    mutable std::vector<ObjT> data_;
    mutable std::vector<bool> del_bitmap_;
    mutable std::unordered_map<typename ObjT::KeyT, size_t> hashed_objs_;

    mutable bool data_in_memory_ = true;
    // number of objects in data vector
    // return size_ when user request 'size' without data vector being in the memory
    mutable size_t size_ = 0;
    mutable size_t sorted_size_ = 0;
    mutable size_t num_del_ = 0;
    int page_size_ = 0;
    mutable int64_t byte_size_ = 0;

    template <typename T>
    friend class ObjList;
};

}  // namespace husky
