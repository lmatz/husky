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

#include <list>
#include <unordered_map>
#include <utility>

#include "boost/optional.hpp"

namespace husky {

template <typename KeyT, typename ValT>
class ICache {
   public:
    explicit ICache(size_t size) { size_ = size; }

    size_t get_max_size() { return size_; }

   protected:
    size_t size_;
};

template <typename KeyT, typename ValT>
class LRUCache : public ICache<KeyT, ValT> {
   public:
    using ListIterator = typename std::list<std::pair<KeyT, ValT>>::iterator;

    explicit LRUCache(size_t size) : ICache<KeyT, ValT>(size) {}

    ~LRUCache() = default;

    template <typename KeyU, typename ValU>
    void put(KeyU&& key, ValU&& val) {
        auto it_cache = cache_map_.find(key);
        cache_list_.push_front(std::make_pair(key, val));
        if (it_cache != cache_map_.end()) {
            cache_list_.erase(it_cache->second);
            cache_map_.erase(it_cache);
        }
        cache_map_[key] = cache_list_.begin();

        if (cache_map_.size() > this->size_) {
            del();
        }
    }

    boost::optional<std::pair<KeyT, ValT>> del() {
        if (cache_map_.size() == 0) {
            return boost::none;
        }
        auto last = cache_list_.end();
        last--;
        KeyT key = last->first;
        ValT objlist = last->second;
        cache_map_.erase(key);
        cache_list_.pop_back();
        return *last;
    }

    boost::optional<std::pair<KeyT, ValT>> poll() {
        if (cache_map_.size() == 0) {
            return boost::none;
        }
        return cache_list_.back();
    }

    size_t get_size() { return cache_map_.size(); }

    template <typename KeyU>
    bool exists(KeyU&& key) {
        return cache_map_.find(std::forward<KeyU>(key)) != cache_map_.end();
    }

   private:
    std::unordered_map<KeyT, ListIterator> cache_map_;
    std::list<std::pair<KeyT, ValT>> cache_list_;
};

template <typename KeyT, typename ValT>
class FIFOCache : public ICache<KeyT, ValT> {
   public:
    using ListIterator = typename std::list<std::pair<KeyT, ValT>>::iterator;

    explicit FIFOCache(size_t size) : ICache<KeyT, ValT>(size) {}

    ~FIFOCache() = default;

    template <typename KeyU, typename ValU>
    void put(KeyU&& key, ValU&& val) {
        auto it_cache = cache_map_.find(key);
        if (it_cache != cache_map_.end() && it_cache->second->second != val) {
            cache_list_.erase(it_cache->second);
            cache_map_.erase(it_cache);
        } else if (it_cache != cache_map_.end() && it_cache->second->second == val) {
            return;
        }
        cache_list_.push_front(std::make_pair(key, val));
        cache_map_[key] = cache_list_.begin();

        if (cache_map_.size() > this->size_) {
            del();
        }
    }

    boost::optional<std::pair<KeyT, ValT>> del() {
        if (cache_map_.size() == 0) {
            return boost::none;
        }
        auto last = cache_list_.end();
        last--;
        KeyT key = last->first;
        ValT objlist = last->second;
        cache_map_.erase(key);
        cache_list_.pop_back();
        return *last;
    }

    boost::optional<std::pair<KeyT, ValT>> poll() {
        if (cache_map_.size() == 0) {
            return boost::none;
        }
        return cache_list_.back();
    }

    template <typename KeyU>
    bool exists(KeyU&& key) {
        return cache_map_.find(key) != cache_map_.end();
    }

    size_t get_size() { return cache_map_.size(); }

   private:
    std::unordered_map<KeyT, ListIterator> cache_map_;
    std::list<std::pair<KeyT, ValT>> cache_list_;
};
}  // namespace husky
