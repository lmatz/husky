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
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "base/disk_store.hpp"
#include "base/serialization.hpp"
#include "core/context.hpp"

namespace husky {

class ObjListBase;

using base::DiskStore;
using base::BinStream;

class Page {
   public:
    using KeyT = size_t;

    explicit Page(KeyT id) : id_(id), tid_(Context::get_local_tid()) {
        this->file_name_ = "/var/tmp/page-" + std::to_string(tid_) + "-" + std::to_string(id_);
        std::string page_size = Context::get_param("page_size");
        std::stringstream str_stream;
        str_stream.str(page_size);
        str_stream >> this->all_bytes_;
        this->free_bytes_ = all_bytes_;
        this->used_bytes_ = 0;
        this->in_memory_ = false;
        this->bin_in_memory_ = false;
    }

    std::string get_file_name() { return file_name_; }

    size_t get_key() { return id_; }

    size_t get_id() { return id_; }

    // called by page store when the page is released finally
    // remove file created temporarily
    void finalize() {
        std::ifstream f(this->file_name_.c_str());
        if (!f.good())
            return;
        int ret = remove(file_name_.c_str());
        if (ret != 0) {
            LOG_I << "Fail to delete a page file " << file_name_;
        }
    }

    // currently not used
    void take_memory(int64_t bytes) {
        used_bytes_ += bytes;
        free_bytes_ -= bytes;
    }

    // currently not used
    void release_memory(int64_t bytes) {
        free_bytes_ += bytes;
        used_bytes_ -= bytes;
    }

    // currently not used
    size_t free_memory() { return free_bytes_; }

    // currently not used
    size_t used_memory() { return used_bytes_; }

    size_t all_memory() { return all_bytes_; }

    const BinStream& get_bin() const { return bs_; }

    BinStream& get_bin() {
        if (!in_memory())
            throw base::HuskyException("a page wants to get_bin when it is not in memory");
        if (!check_bin_in_memory()) {
            read_from_disk();
        }
        return bs_;
    }

    bool check_bin_in_memory() { return bin_in_memory_; }
    // write into the binstream
    // buffered so that it will be finally written onto the disk once for all
    size_t write(const BinStream& bs) {
        size_t ret = bs.size();
        bs_.append(bs);
        return ret;
    }

    size_t write(const std::vector<char>& v) {
        BinStream bs(v);
        return write(bs);
    }

    size_t write(std::vector<char>&& v) {
        BinStream bs(std::move(v));
        return write(bs);
    }

    bool write_to_disk() {
        if (!in_memory_)
            throw base::HuskyException("page " + std::to_string(id_) + " on thread " + std::to_string(tid_) +
                                       " cannot write to disk because it is not in memory.");
        DiskStore ds(file_name_);
        return ds.write(bs_);
    }

    // read the data from disk into binstream
    void read_from_disk() {
        if (!in_memory_)
            throw base::HuskyException("page " + std::to_string(id_) + " on thread " + std::to_string(tid_) +
                                       " cannot read from disk because it is not in memory.");
        DiskStore ds(file_name_);
        bs_ = ds.read();
        bin_in_memory_ = true;
    }

    void clear_bin() {
        bs_.clear();
        bin_in_memory_ = false;
    }

    bool in_memory() { return in_memory_; }

    bool on_disk() { return !in_memory_; }

    size_t get_bin_size() {
        if (!check_bin_in_memory()) {
            read_from_disk();
        }
        return bs_.size();
    }

    // called by memory pool to swap in the page
    // when it put page into the cache owned by memory pool
    void swap_in_memory();
    // called by memory pool to swap out the page
    // when page is deleted by the cached owned by memory pool
    void swap_out_memory();

    void set_owner(ObjListBase* owner);

    ObjListBase* get_owner();

   protected:
    std::string file_name_;
    BinStream bs_;
    bool in_memory_;
    bool bin_in_memory_;
    int64_t all_bytes_;
    int64_t free_bytes_;
    int64_t used_bytes_;

   private:
    size_t id_;
    size_t tid_;
    ObjListBase* owner_ = nullptr;
};

}  // namespace husky
