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

#ifndef _HADOOP_HCE_SUFFIX_MULTI_SEQ_RECORDWRITER_H_
#define _HADOOP_HCE_SUFFIX_MULTI_SEQ_RECORDWRITER_H_

#include "common/hadoop/Hce.hh"
#include "common/hadoop/TemplateFactoryHce.hh"
#include <map>
#include <vector>
#include <string>
using namespace std;

namespace HCE
{
	class SuffixSeqRecordWriter;
	
    class SuffixMultiSeqRecordWriter
	    : public RecordWriter {
	public:
		~SuffixMultiSeqRecordWriter(){};
		virtual int64_t emit(const void* key, uint64_t keyLen, const void *value, uint64_t valueLen);
		virtual int64_t open(TaskContext& context);
		virtual int64_t close();
	protected:
		bool checkSpec(const void* value, uint64_t valueLen);
	private:
	    map<string, SuffixSeqRecordWriter*> _writers;
        static const int SUFFIX_LEN = 2;
		// seperator char and offset of the suffix in value
		static const int SEP_OFFSET = -1;
		static const char SEP = '#';
		// error report string lenth, if value is ill formed, usually print value 
		static int ERR_LEN;
		static const int ERR_LEN_MAX = 128;
		
		// used for store suffix etc
		static char FLAG_MIN;
		static char FLAG_MAX;
		static const int FLAG_OFFSET = 0;
		static string SUFFIX_STRING;
		static vector<string> FILE_SUFFIX;

		TaskContext* _context;
	};
}

#endif
