
#ifndef __HCE_HCE_HH__
#define __HCE_HCE_HH__

/*HERE YOU CAN DEFINE THE LOG_DEVICE debug_file or stderr*/
//#define LOG_DEVICE debug_file
#ifdef SIM_ISO_TEST
#define LOG_DEVICE debug_file
#else
#define LOG_DEVICE stderr
#endif
/**/

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>

namespace HCE {

extern FILE* debug_file;
/**
 * A JobConf defines the properties for a job.
 */
class JobConf {
public:
  /**
   * Test whether key included in JobConf
  */
  virtual bool hasKey(const std::string& key) const = 0;

  /**
   * Get the value corresponding to key in JobConf
  */
  virtual const std::string& get(const std::string& key) const = 0;
  virtual const std::string& get(const std::string& key, const std::string& def) const = 0;
  virtual int getInt(const std::string& key) const = 0;
  virtual int getInt(const std::string& key, const int def) const = 0;
  virtual float getFloat(const std::string& key) const = 0;
  virtual float getFloat(const std::string& key, const float def) const = 0;
  virtual bool getBoolean(const std::string&key) const = 0;
  virtual bool getBoolean(const std::string&key, const bool def) const = 0;

  virtual ~JobConf() {}
};

class OutputCollector {
public:
  virtual int collect(const void *key, int64_t keyLen,
			const void *value, int64_t valueLen) = 0;
  virtual int flush() = 0;
  virtual int close() = 0;

  virtual ~OutputCollector(){};
};
/**
 * Task context provides the information about the task and job.
 */
class TaskContext {
  friend class Mapper;
  friend class Reducer;
  friend class Combiner;
public:
  /**
   * Counter to keep track of a property and its value.
   */
  class Counter {
  private:
    int id;
    std::string* group;
    std::string* name;
  public:
    Counter(int counterId) : id(counterId) { group=NULL; name=NULL; }
    Counter(int counterId, std::string counterGroup, std::string counterName) : id(counterId) {
      group = new std::string(counterGroup);
      name  = new std::string(counterName);
    }
    Counter(const Counter& counter) : id(counter.id) {}

    virtual ~Counter() {
      delete group;
      delete name;
    }

    int getId() const { return id; }
    std::string* getGroup() const { return group; }
    std::string* getName() const { return name; }
  };

  /**
   * Get the JobConf for the current task.
   */
  virtual const JobConf* getJobConf() = 0;

  /**
   * Mark your task as having made progress without changing the status
   * message.
   */
  virtual void progress() = 0;

  /**
   * Set the status message and call progress.
   */
  virtual void setStatus(const std::string& status) = 0;

  /**
   * Register a counter with the given group and name.
   */
  virtual Counter*
    getCounter(const std::string& group, const std::string& name) = 0;

  /**
   * Increment the value of the counter with the given amount.
   */
  virtual void incrementCounter(const Counter* counter, uint64_t amount) = 0;

  /**
   * Access the InputSplit of the mapper.
   * Issue: input_split is used in c++ framework, not for map function
   */
  virtual const std::string& getInputSplit() = 0;

  virtual ~TaskContext() {}

protected:
  /**
   * Generate an output record
   */
  virtual void emit(const void* key, int64_t keyLength,
				const void* value, int64_t valueLength) = 0;

};

/**
 * Input format for map function.
 */
class MapInput {
public:
    /**
     * Get the current key
     *
     * @param size(output parameter) the length of the key
     * @return the address to the key
     */
    virtual const void* key(int64_t& size) const = 0;
    /**
     * Get the current value
     *
     * @param size(output parameter) the length of the value
     * @return the address to the value
     */
    virtual const void* value(int64_t& size) const = 0;
};

/**
 * Input format for reduce function.
 *
 * The typical usage is as follows:
 * <pre>
 *   virtual int64_t reduce(ReduceInput &input) {
 *     int64_t keyLen = 0;
 *     void* key = input.key(keyLen);
 *     while(input.next_value()) {
 *       int64_t valueLen = 0;
 *       void* value = input.value(valueLen);
 *       ...
 *     }
 *   }
 * </pre>
 * The {@link Reducer::reduce} function is the member of {@link Reducer} class
 *
 */
class ReduceInput  {
public:
    /**
     * Get the current key
     *
     * @param size(output parameter) the length of the key
     * @return the address to the key
     */
    virtual const void* key(int64_t& size) = 0;
    /**
     * Get the current value
     *
     * @param size(output parameter) the length of the value
     * @return the address to the value
     */
    virtual const void* value(int64_t& size) = 0;
    /**
     * Go to the next value
     *
     * @return true next value is OK
     * @return false the end
     */
    virtual bool nextValue() = 0;
protected:
    /**
     * Go to the next key
     *
     * @return true next key is OK
     * @return false the end
     */
    virtual bool nextKey() = 0;
};

/**
 * The application's mapper class to do map.
 */
class Mapper {
  friend class TaskContextImpl;
public:
  /**
   * Called once at the beginning of the task.
   *
   * @return 0 successful
   * @return others failed, something wrong
   */
  virtual int64_t setup() { return 0; }
  /**
   * Called once at the end of the task.
   *
   * @param isSuccessful the just round is successful or failed
   * @return 0 successful
   * @return others failed, something wrong
   */
  virtual int64_t cleanup() { return 0; }
  /**
   * Get a single input key/value pair,
   * and {@link emit} one or more key/value pairs into intermediate store.
   *
   * @param input a {@link MapInput} reference containing key/value pair
   * @return 0 successful
   * @return others failed, something wrong
   */
  virtual int64_t map(MapInput &input) = 0;

  virtual ~Mapper(){}

protected:
  /**
   * Called in map function to emit intermediate key/value pairs
   *
   * @param key address to intermediate key
   * @param keyLength length of intermediate key
   * @param value address to intermediate value
   * @param valueLength length of intermediate value
   */
  virtual void emit(const void* key, int64_t keyLength,
                const void* value, int64_t valueLength) {
    getContext()->emit(key, keyLength, value, valueLength);
  }
  /**
   * Get the task context
   *
   * @return a pointer to {@link TaskContext}
   */
  virtual TaskContext* getContext() {
    return context;
  }

private:
  int64_t InnerInitialize(TaskContext& taskContext) {
    context = &taskContext;
    return 0;
  }

  TaskContext* context;
};

/**
 * The application's reducer class to do reduce.
 */
class Reducer {
  friend class TaskContextImpl;
public:
  /**
   * Called once at the beginning of the task.
   *
   * @return 0 successful
   * @return others failed, something wrong
   */
  virtual int64_t setup() { return 0; }
  /**
   * Called once at the end of the task.
   *
   * @param isSuccessful the just round is successful or failed
   * @return 0 successful
   * @return others failed, something wrong
   */
  virtual int64_t cleanup() { return 0; }
  /**
   * Get a single input key along with a sequence of values
   * and {@link emit} one or more key/value pairs into final store.
   *
   * @param input a {@link ReduceInput} reference
   * @return 0 successful
   * @return others failed, something wrong
   */
  virtual int64_t reduce(ReduceInput &input) = 0;

  virtual ~Reducer(){}

protected:
  /**
   * Called in reduce function to emit final key/value pairs
   *
   * @param key address to final key
   * @param keyLength length of final key
   * @param value address to final value
   * @param valueLength length of final value
   */
  virtual void emit(const void* key, int64_t keyLength,
                const void* value, int64_t valueLength) {
    getContext()->emit(key, keyLength, value, valueLength);
  }
  /**
   * Get the task context
   *
   * @return a pointer to {@link TaskContext}
   */
  virtual TaskContext* getContext() {
    return context;
  }

private:
  int64_t InnerInitialize(TaskContext& taskContext) {
    context = &taskContext;
    return 0;
  }

  TaskContext* context;
};

/**
 * The application's combiner class to do combine.
 */
class Combiner {
  friend class MapOutputCollector;
public:
  /**
   * Called once at the beginning of the combine
   */
  virtual int64_t setup() { return 0; }

  /**
   * Called once at the end of the combine
   *
   * @param isSuccessful the just round is successful or failed
   */
  virtual int64_t cleanup() { return 0; }
  /**
   * Get a single input key along with a sequence of values
   * and {@link emit} the combined value
   *
   * @param input a {@link ReduceInput} reference
   * @return 0 successful
   * @return others failed, something wrong
   */
  virtual int64_t combine(ReduceInput &input) = 0;

  virtual ~Combiner(){}

protected:
  /**
   * Called in combine function to emit combined value
   *
   * @param value address to combined value
   * @param valueLength length of combined value
   */
  virtual void emit(const void* value, int64_t valueLength) {
    getContext()->emit(getCombineKey(), getCombineKeyLength(), value, valueLength);
  }
  /**
   * Get the task context
   *
   * @return a pointer to {@link TaskContext}
   */
  virtual TaskContext* getContext() {
    return context;
  }
  /**
   * Get the key to the current combining
   *
   * @return the address to combing key
   */
  virtual const void* getCombineKey() {
    return combineKey;
  }
  /**
   * Get the key length to the current combining
   *
   * @return the address to length of combing key
   */
  virtual int64_t getCombineKeyLength() {
    return combineKeyLength;
  }

private:
  int64_t InnerInitialize(TaskContext& taskContext) {
    context = &taskContext;
    combineKey = NULL;
    combineKeyLength = -1;
    return 0;
  }
  int64_t InnerCombine(ReduceInput &input) {
    combineKey = input.key(combineKeyLength);
    return combine(input);
  }
  TaskContext* context;
  const void* combineKey;
  int64_t combineKeyLength;
};

/**
 * User code to decide where each key should be sent.
 */
class Partitioner {
  friend class TaskContextImpl;
public:
  /**
   * Called once at the beginning of the partition
   */
  virtual int64_t setup() { return 0; }
  /**
   * Called once at the end of the partition
   */
  virtual int64_t cleanup() { return 0; }
  /**
   * Get the partition result
   *
   * @param key address to the current key
   * @param KeyLength length of the current key
   * @param numOfReduces the number of reduces
   * @return the partition result
   */
  virtual int32_t partition(void* key, int64_t &keyLength, int numOfReduces) = 0;

  virtual ~Partitioner() {}

protected:
  /**
   * Get the task context
   *
   * @return a pointer to {@link TaskContext}
   */
  virtual TaskContext* getContext() {
    return context;
  }

private:
  int64_t InnerInitialize(TaskContext& taskContext) {
    context = &taskContext;
    return 0;
  }

  TaskContext* context;
};

/**
 * User code to decide where each key should be sent.
 */
class Committer {
public:
  virtual int commit() = 0;
  virtual void abortTask() = 0;
  virtual bool needsTaskCommit() = 0;
  virtual ~Committer() {}
};

/**
 * For applications that want to read the input directly for the map function
 * they can define RecordReaders in C++.
 */
class RecordReader {
public:
  /**
   * Get a next key-value
   */
  virtual int next(void*& key, int64_t& keyLength,
            void*& value, int64_t& valueLength) = 0;
  /**
   * The progress of the record reader through the split as a value between
   * 0.0 and 1.0.
   */
  virtual float getProgress() = 0;
  virtual int64_t open(TaskContext& context) = 0;
  virtual int64_t close() = 0;
  virtual ~RecordReader(){};
};

/**
 * An object to write key/value pairs as they are emited from the reduce.
 */
class RecordWriter {
public:
  /**
   * Emit a key-value to destination, through single stream or mutiple stream.
   */
  virtual int64_t emit(const void* key, uint64_t keyLength,
			const void* value, uint64_t valueLength) = 0;
  virtual int64_t open(TaskContext& context) = 0;
  virtual int64_t close() = 0;

  virtual ~RecordWriter(){}
};

/**
 * A factory to create the necessary application objects.
 */
class Factory {
public:
  virtual Mapper* createMapper() const = 0;
  virtual Reducer* createReducer() const = 0;

  /**
   * Create a combiner, if this application has one.
   * @return the new combiner or NULL, if one is not needed
   */
  virtual Combiner* createCombiner() const {
    return NULL;
  }

  /**
   * Create an application partitioner object.
   * @return the new partitioner or NULL, if the default partitioner should be
   *     used.
   */
  virtual Partitioner* createPartitioner() const {
    return NULL;
  }

  /**
   * Create an application committer object.
   * @return the new committer or NULL, if the default committer should be
   *     used.
   */
  virtual Committer* createCommitter() const {
    return NULL;
  }

  /**
   * Create an application record reader.
   * @return the new RecordReader or NULL, if the Java RecordReader should be
   *    used.
   */
  virtual RecordReader* createRecordReader() const {
    return NULL;
  }

  /**
   * Create an application record writer.
   * @return the new RecordWriter or NULL, if the Java RecordWriter should be
   *    used.
   */
  virtual RecordWriter* createRecordWriter() const {
    return NULL;
  }

  virtual ~Factory() {}
};

/**
 * Run the assigned task in the framework.
 * The user's main function should set the various functions using the
 * set* functions above and then call this.
 * @return true, if the task succeeded.
 */
bool runTask(const Factory & factory);

}

#include "common/hadoop/TemplateFactoryHce.hh"
#include "common/hadoop/SequenceFileInputOutput.hh"
#include "common/hadoop/HashPartitioner.hh"
#include "common/hadoop/IntHashPartitioner.hh"
#include "common/hadoop/MapIntPartitioner.hh"
#include "common/hadoop/KeyFieldBasedPartitioner.hh"
#include "common/hadoop/SuffixMultiTextRecordWriter.hh"
#include "common/hadoop/SuffixMultiSeqRecordWriter.hh"

#endif  // __HCE_HCE_HH__
