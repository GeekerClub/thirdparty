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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef  __MAPINTPARTITIONER_HH_
#define  __MAPINTPARTITIONER_HH_

#include <stdint.h>

namespace HCE {

  class MapIntPartitioner: public Partitioner {
    public:
      virtual int32_t partition(void* key, int64_t &keyLength, int numOfReduces) {
        char * end = (char *)key + keyLength - 1;
        long count=0;
        long base=1;
        while(end>=(char*)key && *end!=' ') {
          count += (*end-'0') * base;
          base*=10;
          --end;
        }
        if (end>=(char*)key){
          *end = '\0';
          keyLength = (end - (char*)key);
          return count % numOfReduces;
        }
        return 0;
      }

      int64_t setup() {       
        return 0;
      }
      int64_t cleanup() {       
        return 0;
      }
  };

}


#endif  //__MAPINTPARTITIONER_HH_
/* vim: set ts=2 sw=2 sts=2 tw=100 et: */
