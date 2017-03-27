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

#include <sys/sysinfo.h>
#include <sys/types.h>

namespace husky {
namespace base {
class Memory {
   public:
    static size_t total_phys_mem();
    static size_t total_virtual_mem();

   private:
    static struct sysinfo mem_info;
};
}  // namespace base
}  // namespace husky
