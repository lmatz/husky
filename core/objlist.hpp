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

#include <algorithm>
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "boost/random.hpp"

#include "base/assert.hpp"
#include "base/exception.hpp"
#include "base/serialization.hpp"
#include "core/attrlist.hpp"
#include "core/channel/channel_destination.hpp"
#include "core/channel/channel_source.hpp"
#include "core/objlist_data.hpp"

namespace husky {

using base::BinStream;
using base::DiskStore;

class ObjListBase : public ChannelSource, public ChannelDestination {
   public:
    ObjListBase() : id_(s_counter) { s_counter++; }

    virtual ~ObjListBase() = default;

    ObjListBase(const ObjListBase&) = delete;
    ObjListBase& operator=(const ObjListBase&) = delete;

    ObjListBase(ObjListBase&&) = delete;
    ObjListBase& operator=(ObjListBase&&) = delete;

    // TODO(legend): Consider how to add the index of thread_id.
    inline std::string id2str() const { return "ObjList-" + std::to_string(id_); }

    inline size_t get_id() const { return id_; }

    virtual size_t get_size() const = 0;

    virtual void sort() = 0;

    virtual void deletion_finalize() = 0;

    virtual void clear_page_from_memory(Page* page) = 0;

   private:
    size_t id_;

    static thread_local size_t s_counter;
};

template <typename ObjT>
class ObjList : public ObjListBase {
   public:
    ObjList() = default;

    virtual ~ObjList() {
        for (auto& it : this->attrlist_map)
            delete it.second;
        this->attrlist_map.clear();
    }

    ObjList(const ObjList&) = default;
    ObjList& operator=(const ObjList&) = default;

    ObjList(ObjList&&) = default;
    ObjList& operator=(ObjList&&) = default;

    std::vector<ObjT>& get_data() { return objlist_data_.get_data(); }

    std::vector<bool>& get_del_bitmap() { return objlist_data_.get_del_bitmap(); }

    void sort() {
        auto& data = objlist_data_.get_data();
        if (data.size() == 0)
            return;
        std::vector<int> order(this->get_size());
        for (int i = 0; i < order.size(); ++i) {
            order[i] = i;
        }
        // sort the permutation
        std::sort(order.begin(), order.end(),
                  [&](const size_t a, const size_t b) { return data[a].id() < data[b].id(); });
        // apply the permutation on all the attribute lists
        for (auto& it : this->attrlist_map)
            it.second->reorder(order);
        std::sort(data.begin(), data.end(), [](const ObjT& a, const ObjT& b) { return a.id() < b.id(); });
        auto& hashed_objs = objlist_data_.get_hashed_objs();
        hashed_objs.clear();
        auto& sorted_size = objlist_data_.get_sorted_size();
        sorted_size = data.size();
    }

    void deletion_finalize() {
        auto& data = objlist_data_.get_data();
        auto& del_bitmap = objlist_data_.get_del_bitmap();
        if (data.size() == 0)
            return;
        size_t i = 0, j;
        // move i to the first empty place
        while (i < data.size() && !del_bitmap[i])
            i++;

        if (i == data.size())
            return;

        for (j = data.size() - 1; j > 0; j--) {
            if (!del_bitmap[j]) {
                data[i] = std::move(data[j]);
                // move j_th attribute to i_th for all attr lists
                for (auto& it : this->attrlist_map)
                    it.second->move(i, j);
                i += 1;
                // move i to the next empty place
                while (i < data.size() && !del_bitmap[i])
                    i++;
            }
            if (i >= j)
                break;
        }
        data.resize(j);
        del_bitmap.resize(j);
        for (auto& it : this->attrlist_map)
            it.second->resize(j);
        auto& num_del = objlist_data_.get_num_del();
        num_del = 0;
        std::fill(del_bitmap.begin(), del_bitmap.end(), 0);
        BinStream bs;
        bs << data;
        objlist_data_.byte_size_ = bs.size();
    }

    size_t delete_object(const ObjT* const obj_ptr) { return objlist_data_.delete_object(obj_ptr); }

    size_t delete_object(const typename ObjT::KeyT& key) { return objlist_data_.delete_object(key); }

    ObjT* find(const typename ObjT::KeyT& key) {
        auto& working_list = objlist_data_.get_data();
        auto& sorted_size = objlist_data_.get_sorted_size();
        if (working_list.size() == 0)
            return nullptr;
        ObjT* start_addr = &working_list[0];
        int r = sorted_size - 1;
        int l = 0;
        int m = (r + l) / 2;

        while (l <= r) {
// __builtin_prefetch(start_addr+(m+1+r)/2, 0, 1);
// __builtin_prefetch(start_addr+(l+m-1)/2, 0, 1);
#ifdef ENABLE_LIST_FIND_PREFETCH
            __builtin_prefetch(&(start_addr[(m + 1 + r) / 2].id()), 0, 1);
            __builtin_prefetch(&(start_addr[(l + m - 1) / 2].id()), 0, 1);
#endif
            // __builtin_prefetch(&working_list[(m+1+r)/2], 0, 1);
            // __builtin_prefetch(&working_list[(l+m-1)/2], 0, 1);
            auto tmp = start_addr[m].id();
            if (tmp == key)
                return &working_list[m];
            else if (tmp < key)
                l = m + 1;
            else
                r = m - 1;
            m = (r + l) / 2;
        }

        auto& data = objlist_data_.get_data();
        auto& hashed_objs = objlist_data_.get_hashed_objs();
        // The object to find is not in the sorted part
        if ((sorted_size < data.size()) && (hashed_objs.find(key) != hashed_objs.end()))
            return &(data[hashed_objs[key]]);
        return nullptr;
    }

    size_t index_of(const ObjT* const obj_ptr) { return objlist_data_.index_of(obj_ptr); }

    size_t index_of(const typename ObjT::KeyT& key) { return objlist_data_.index_of(key); }

    template <typename ObjU>
    size_t add_object(ObjU&& obj) {
        if (!objlist_data_.check_data_in_memory()) {
            DLOG_I << "data not in memory, bring data from disk";
            objlist_data_.read_data_from_disk();
        }
        BinStream bs;
        bs << obj;
        objlist_data_.byte_size_ += bs.size();
        size_t num_pages = objlist_data_.pages_.size();
        while (objlist_data_.byte_size_ > objlist_data_.page_size_ * num_pages) {
            DLOG_I << "not enough pages";
            Page* new_page = PageStore::create_page();
            new_page->set_owner(this);
            DLOG_I << "request new page";
            MemoryPool::get_mem_pool().request_page(new_page->get_key(), new_page);
            DLOG_I << "push_back new page into objlist_data";
            objlist_data_.pages_.push_back(new_page);
            num_pages += 1;
        }
        size_t ret = objlist_data_.get_hashed_objs()[obj.id()] = objlist_data_.get_data().size();
        objlist_data_.get_data().push_back(std::forward<ObjU>(obj));
        objlist_data_.get_del_bitmap().push_back(0);
        return ret;
    }

    bool get_del(size_t idx) const { return objlist_data_.del_bitmap_[idx]; }

    // Create AttrList
    template <typename AttrT>
    AttrList<ObjT, AttrT>& create_attrlist(const std::string& attr_name, const AttrT& default_attr = {}) {
        if (attrlist_map.find(attr_name) != attrlist_map.end())
            throw base::HuskyException("ObjList<T>::create_attrlist error: name already exists");
        auto* attrlist = new AttrList<ObjT, AttrT>(&objlist_data_, default_attr);
        attrlist_map.insert({attr_name, attrlist});
        return (*attrlist);
    }

    template <typename AttrT>
    AttrList<ObjT, AttrT>& get_attrlist(const std::string& attr_name) {
        if (attrlist_map.find(attr_name) == attrlist_map.end())
            throw base::HuskyException("ObjList<T>::get_attrlist error: AttrList does not exist");
        return (*static_cast<AttrList<ObjT, AttrT>*>(attrlist_map[attr_name]));
    }

    // Delete AttrList
    size_t del_attrlist(const std::string& attr_name) {
        if (attrlist_map.find(attr_name) != attrlist_map.end())
            delete attrlist_map[attr_name];
        return attrlist_map.erase(attr_name);
    }

    void migrate_attribute(BinStream& bin, const size_t idx) {
        if (!this->attrlist_map.empty())
            for (auto& item : this->attrlist_map)
                item.second->migrate(bin, idx);
    }

    void process_attribute(BinStream& bin, const size_t idx) {
        if (!this->attrlist_map.empty())
            for (auto& item : this->attrlist_map)
                item.second->process_bin(bin, idx);
    }

    size_t estimated_storage_size(const double sample_rate = 0.005) {
        if (this->get_vector_size() == 0)
            return 0;
        const size_t sample_num = this->get_vector_size() * sample_rate + 1;
        BinStream bs;

        // sample
        std::unordered_set<size_t> sample_container;
        boost::random::mt19937 generator;
        boost::random::uniform_real_distribution<double> distribution(0.0, 1.0);
        while (sample_container.size() < sample_num) {
            size_t index = distribution(generator) * objlist_data_.get_vector_size();
            sample_container.insert(index);
        }

        // log the size
        for (auto iter = sample_container.begin(); iter != sample_container.end(); ++iter)
            bs << objlist_data_.get_data()[*iter];

        std::vector<ObjT>& v = objlist_data_.get_data();
        size_t ret = bs.size() * sizeof(char) * v.capacity() / sample_num;
        return ret;
    }

    inline size_t get_sorted_size() const { return objlist_data_.get_sorted_size(); }
    inline size_t get_num_del() const { return objlist_data_.get_num_del(); }
    inline size_t get_hashed_size() const { return objlist_data_.get_hashed_size(); }
    inline size_t get_size() const override { return objlist_data_.get_size(); }
    inline size_t get_vector_size() const { return objlist_data_.get_vector_size(); }
    inline ObjT& get(size_t i) { return objlist_data_.get(i); }

    bool check_data_in_memory() { return objlist_data_.check_data_in_memory(); }

   protected:
    // this method is called by page owned by this objlist_data when the page is about to be swapped out of the memory
    // since currently only supporting the swapping of the whole objlist_data
    // so when the first page is about to be swapped out of the memory
    // we write the data vector into these pages
    // then these pages write into their corresponding file on the disk
    void clear_page_from_memory(Page* page) {
        DLOG_I << "objlist " << this->get_id() << " clear page from memory get called";
        bool data_in_mem = objlist_data_.check_data_in_memory();
        bool pages_in_mem = objlist_data_.check_pages_in_memory();
        // check whether this is the first page owned by this objlist to be swapped out of the memory
        if (data_in_mem && pages_in_mem) {
            DLOG_I << "objlist " << this->get_id() << " clear page from memory get called and we write data to disk";
            deletion_finalize();
            sort();
            objlist_data_.write_to_disk();
        }
    }

    ObjListData<ObjT> objlist_data_;
    std::unordered_map<std::string, AttrListBase*> attrlist_map;

    friend class Page;
};
}  // namespace husky
