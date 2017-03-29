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

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "base/disk_store.hpp"
#include "base/log.hpp"
#include "base/serialization.hpp"
#include "base/time.hpp"
#include "core/context.hpp"

namespace husky {

// ObjListBase is located in the file core/objlist.hpp.
// Forward declaration is used to prevent cyclic header file inclusion.
class ObjListBase;

using base::DiskStore;
using base::BinStream;

class Page {
   public:
    using KeyT = size_t;

    // TODO(lmatz): setting the path to keep temporary files.
    explicit Page(KeyT id, size_t page_size) : id_(id), all_bytes_(page_size) {
        this->file_name_ =
            "/var/tmp/page-" + std::to_string(base::get_current_time_milliseconds()) + "-" + std::to_string(id_);
        this->in_memory_ = false;
        this->bin_in_memory_ = false;
    }

    bool write_to_disk() {
        if (!in_memory_)
            throw base::HuskyException("page " + file_name_ + " cannot write to disk because it is not in memory.");
        if (bs_.size()==0)
            return true;
        DiskStore ds(file_name_);
        BinStream tmp_bs(bs_);
        return ds.write(std::move(tmp_bs));
    }

    // read the data from disk into binstream
    void read_from_disk() {
        if (!in_memory_)
            throw base::HuskyException("page " + file_name_ + " cannot read from disk because it is not in memory.");
        DiskStore ds(file_name_);
        bs_ = ds.read();
        bin_in_memory_ = true;
    }

    // called by page store when the page is released finally
    // remove file created temporarily
    void finalize() {
        // check the existence of the file
        std::ifstream f(this->file_name_.c_str());
        if (!f.good())
            return;
        int ret = remove(file_name_.c_str());
        if (ret != 0) {
            LOG_I << "Fail to delete a page file " << file_name_;
        }
    }

    inline size_t all_memory() { return all_bytes_; }

    inline const BinStream& get_bin() const { return bs_; }

    inline BinStream& get_bin() {
        if (!in_memory())
            throw base::HuskyException("a page wants to get_bin when it is not in memory");
        if (!check_bin_in_memory()) {
            read_from_disk();
        }
        return bs_;
    }

    inline bool check_bin_in_memory() { return bin_in_memory_; }
    // write into the binstream
    // buffered so that it will be finally written onto the disk once for all
    inline size_t write(const BinStream& bs) {
        size_t ret = bs.size();
        bs_.append(bs);
        bin_in_memory_ = true;
        return ret;
    }

    inline size_t write(const std::vector<char>& v) {
        BinStream bs(v);
        return write(bs);
    }

    inline size_t write(std::vector<char>&& v) {
        BinStream bs(std::move(v));
        return write(bs);
    }

    inline void clear_bin() {
        bs_.clear();
        bin_in_memory_ = false;
    }

    inline bool in_memory() { return in_memory_; }

    inline bool on_disk() { return !in_memory_; }

    inline size_t get_bin_size() {
        if (!check_bin_in_memory()) {
            read_from_disk();
        }
        return bs_.size();
    }

    inline std::string get_file_name() { return file_name_; }

    inline size_t get_key() { return id_; }

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
    size_t all_bytes_;

   private:
    size_t id_;
    ObjListBase* owner_ = nullptr;
};

}  // namespace husky
