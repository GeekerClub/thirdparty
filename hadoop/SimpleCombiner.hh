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

#ifndef __HCE_SIMPLECOMBINER_HH__
#define __HCE_SIMPLECOMBINER_HH__

#include <stdint.h>
#include <string>
using std::string;

#define MAX_VS_NUM 128
namespace HCE {
  /**
   * Combine values by simple aggregation of literal demical integers
   * e.g.  value seq 1\t2\t3, 4\t5\t6 combines to 5\t7\t9 with separator \t
   */
  class SimpleCombiner: public Combiner {
  protected:
    long _vs[MAX_VS_NUM];
    char _tempbuff[MAX_VS_NUM*32];
    long _vsn;
    char _sepChar;
  public:
    virtual int64_t setup() {
      string _sep = getContext()->getJobConf()->get("stream.map.output.field.separator", "\t");
      if (1==_sep.length()){
        _sepChar = _sep[0];
        return 0;
      }else{
        fprintf(stderr, "FATAL: SimpleCombiner only works when length(stream.map.output.field.separator)==1, current: [%s]\n", _sep.c_str());
        return 1;
      }
    }
    virtual int64_t combine(ReduceInput &input) {
      _vsn = 0;
      while(input.nextValue()){
        int64_t buflen;
        const char * buf = (char*)input.value(buflen);
        if (0==buflen)
          continue;
        long curv = 0;
        long v = 0;
        for (int64_t i=0;i<buflen;++i){
          if (buf[i]==_sepChar){
            if(curv==_vsn){
              if (_vsn==MAX_VS_NUM){
                fprintf(stderr,"FATAL: SimpleCombiner only have %d aggregation slots, but got value: [%s]\n", MAX_VS_NUM, buf);
                return 1;
              }
              _vs[_vsn] = 0;
              ++_vsn;
            }
            _vs[curv]+=v;
            v = 0;
            ++curv;
          }else{
            if (buf[i]>='0' && buf[i]<='9'){
              v = v*10 + (buf[i]-'0');
            }else{
              fprintf(stderr,"FATAL: Format error! SimpleCombiner only accept digits, but got value: [%s]\n", buf);
              return 1;
            }
          }
        }
        if(curv==_vsn){
          if (_vsn==MAX_VS_NUM){
            fprintf(stderr,"FATAL: SimpleCombiner only have %d aggregation slots, but got value: [%s]\n", MAX_VS_NUM, buf);
            return 1;
          }
          _vs[_vsn] = 0;
          ++_vsn;
        }
        _vs[curv]+=v;
      }
      // format output
      char * st = _tempbuff;
      for (long i=0;i<_vsn;i++){
        if (i){
          *st = _sepChar;
          ++st;
        }
        int s = snprintf(st,32,"%lu",_vs[i]);
        st+=s;
      }
      emit(_tempbuff,st-_tempbuff);
      return 0;
    }
  };
}

#endif  //__HCE_SIMPLECOMBINER_HH__
