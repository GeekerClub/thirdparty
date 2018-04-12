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
#ifndef __HCE_TEMPLATE_FACTORY_HH__
#define __HCE_TEMPLATE_FACTORY_HH__

namespace HCE {

  template <class mapper, class reducer>
  class TemplateFactory2: public Factory {
  public:
    Mapper* createMapper() const {
      return new mapper();
    }
    Reducer* createReducer() const {
      return new reducer();
    }
  };

  template <class mapper, class reducer, class partitioner>
  class TemplateFactory3: public TemplateFactory2<mapper,reducer> {
  public:
    Partitioner* createPartitioner() const {
      return new partitioner();
    }
  };

  template <class mapper, class reducer>
  class TemplateFactory3<mapper, reducer, void>
      : public TemplateFactory2<mapper,reducer> {
  };

  template <class mapper, class reducer, class partitioner, class combiner>
  class TemplateFactory4
   : public TemplateFactory3<mapper,reducer,partitioner>{
  public:
    Combiner* createCombiner() const {
      return new combiner();
    }
  };

  template <class mapper, class reducer, class partitioner>
  class TemplateFactory4<mapper,reducer,partitioner,void>
   : public TemplateFactory3<mapper,reducer,partitioner>{
  };

  template <class mapper, class reducer, class partitioner, 
            class combiner, class committer>
  class TemplateFactory5
   : public TemplateFactory4<mapper,reducer,partitioner,combiner>{
  public:
    Committer* createCommitter() const {
      return new committer();
    }
  };

  template <class mapper, class reducer, class partitioner,class combiner>
  class TemplateFactory5<mapper,reducer,partitioner,combiner,void>
   : public TemplateFactory4<mapper,reducer,partitioner,combiner>{
  };

  template <class mapper, class reducer, class partitioner, 
            class combiner, class committer, class recordReader>
  class TemplateFactory6
   : public TemplateFactory5<mapper,reducer,partitioner,combiner,committer>{
  public:
    RecordReader* createRecordReader() const {
      return new recordReader();
    }
  };

  template <class mapper, class reducer, class partitioner,class combiner,class committer>
  class TemplateFactory6<mapper,reducer,partitioner,combiner,committer,void>
   : public TemplateFactory5<mapper,reducer,partitioner,combiner,committer>{
  };

  template <class mapper, class reducer, class partitioner=void, 
            class combiner=void, class committer=void, class recordReader=void, 
            class recordWriter=void> 
  class TemplateFactory
   : public TemplateFactory6<mapper,reducer,partitioner,combiner,committer,recordReader>{
  public:
    RecordWriter* createRecordWriter() const {
      return new recordWriter();
    }
  };

  template <class mapper, class reducer, class partitioner, 
            class combiner, class committer, class recordReader>
  class TemplateFactory<mapper, reducer, partitioner, combiner, committer, recordReader, 
                        void>
   : public TemplateFactory6<mapper,reducer,partitioner,combiner,committer,recordReader>{
  };

}

#endif  // __HCE_TEMPLATE_FACTORY_HH__
