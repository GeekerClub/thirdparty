
#ifndef  __HCE_SEQUENCEFILEINPUTOUTPUT_HH__
#define  __HCE_SEQUENCEFILEINPUTOUTPUT_HH__

namespace HCE {

  class SequenceFileReader;
  class SequenceFileWriter;

  class SequenceRecordReader : public RecordReader {
  public:
    SequenceRecordReader();
    virtual ~SequenceRecordReader();
    virtual int next(void*& key, int64_t& keyLength, void*& value, int64_t& valueLength);
    virtual float getProgress();
    virtual int64_t open(TaskContext& context);
    virtual int64_t close();
  private:
    SequenceFileReader *_reader;
  };

  class SequenceRecordWriter : public RecordWriter {
  public:
    SequenceRecordWriter();
    virtual ~SequenceRecordWriter();
    virtual int64_t emit(int nStream, const void* key, uint64_t keyLength,
        const void* value, uint64_t valueLength);
    virtual int64_t emit(const void* key, uint64_t keyLength,
        const void* value, uint64_t valueLength);
    virtual int64_t open(TaskContext& context);
    virtual int64_t close();
  private:
    SequenceFileWriter *_writer;
  };

}

#endif  //__HCE_SEQUENCEFILEINPUTOUTPUT_HH__

/* vim: set ts=2 sw=2 sts=2 tw=100 et: */
