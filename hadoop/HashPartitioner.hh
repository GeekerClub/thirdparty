/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __HCE_HASHPARTITIONER_HH__
#define __HCE_HASHPARTITIONER_HH__

#include <stdint.h>

namespace HCE {
  /**
   * Partition keys by their {@link Object#hashCode()}. 
   */
  class HashPartitioner: public Partitioner {
  public:
    HashPartitioner() {}
    ~HashPartitioner() {}

    int64_t setup() {return 0;}

    int64_t cleanup() {return 0;}

    virtual int32_t partition(void* key, int64_t &keyLength, int numOfReduces);
  };
}

#endif  //__HCE_HASHPARTITIONER_HH__

