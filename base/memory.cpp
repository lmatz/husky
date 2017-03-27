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

#include "base/memory.hpp"

#include <sys/sysinfo.h>

namespace husky {
namespace base {

struct sysinfo Memory::mem_info;

size_t Memory::total_phys_mem() {
    sysinfo(&mem_info);
    size_t total_phys_mem = mem_info.totalram;
    total_phys_mem *= mem_info.mem_unit;
    return total_phys_mem;
}

size_t Memory::total_virtual_mem() {
    sysinfo(&mem_info);
    size_t total_vir_mem = mem_info.totalram;
    total_vir_mem += mem_info.totalswap;
    total_vir_mem *= mem_info.mem_unit;
    return total_vir_mem;
}

}  // namespace base
}  // namespace husky
