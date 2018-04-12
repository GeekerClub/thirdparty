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

#ifndef  __KEYFIELDBASEDPARTITIONER_HH_
#define  __KEYFIELDBASEDPARTITIONER_HH_

#include <stdint.h>
#include <string>
#include <vector>

namespace HCE
{
  struct KeyDesc
  {
    int _start;
    int _end;
    int _start_offset;
    int _end_offset;
    KeyDesc(int start=0, int end=-1):_start(start),_end(end),_start_offset(0),_end_offset(-1){}
    bool parse_from_option(const std::string & option);
  };
  /**
   *  Defines a way to partition keys based on certain key fields (also see
   *  {@link KeyFieldBasedComparator}.
   *  The key specification supported is of the form -k pos1[,pos2], where,
   *  pos is of the form f[.c][opts], where f is the number
   *  of the key field to use, and c is the number of the first character from
   *  the beginning of the field. Fields and character posns are numbered
   *  starting with 1; a character position of zero in pos2 indicates the
   *  field's last character. If '.c' is omitted from pos1, it defaults to 1
   *  (the beginning of the field); if omitted from pos2, it defaults to 0
   *  (the end of the field).
   *
   */
  class KeyFieldBasedPartitioner: public Partitioner {
    public:
	    KeyFieldBasedPartitioner():_keyFieldSeparator("\t"){}

      virtual int32_t partition(void* key, int64_t &keyLength, int numOfReduces);

      int64_t setup();

      int64_t cleanup() {
        return 0;
      }

      void setKeyFieldSeparator(const std::string & sep)
      {
        _keyFieldSeparator = sep;
      }

      bool configure_from_option(const std::string & option);
    private:
      std::string _keyFieldSeparator;
      std::vector<KeyDesc> _keyDescs;
      std::vector<size_t> _tempVec;
  };
}



#endif  //__KEYFIELDBASEDPARTITIONER_HH_
/* vim: set ts=2 sw=2 sts=2 tw=100 et: */
