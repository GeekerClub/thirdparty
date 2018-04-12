/** \mainpage
 *
 * <p> <b>Hadoop C++ Extension</b> allows people write MapReduce application in C++ language. The
 * difference between this and pipes is that map output and reduce input are also handled in C++
 * language. In Hadoop C++ Extension, the Java code only manipulate the initialization and task
 * assignment. Data flows through C++ code. </p>
 * 
 * <p> Following pipes, we add a new entry with the name <b>"HCE"</b> in hadoop command, which meaning
 * <b>H</b>adoop <b>C</b>++ <b>E</b>xtension. Command parameter inherits from pipes too and looks
 * like:
 *
 * <pre>
 *   bin/hadoop hce
 *     [-input <path>]                                   specify input directory
 *     [-output <path>]                                  specify output directory
 *     [-jar \<jar file\>                                  specify jar filename
 *     [-program <executable>]                           specify executable URI
 *     [-reduces <num>]                                  specify number of reduces
 *     [-conf \<configuration file\>]                      specify an application configuration file
 *     [-D <property=value>]                             use value for given property
 *     [-fs <local|namenode:port>]                       specify a namenode
 *     [-jt <local|jobtracker:port>]                     specify a job tracker
 *     [-files \<comma separated list of files\>]          specify comma separated files to be copied to the map reduce cluster
 *     [-libjars \<comma separated list of jars\>]         specify comma separated jar files to include in the classpath.
 *     [-archives \<comma separated list of archives\>]    specify comma separated archives to beunarchived on the compute machin
 * </pre>
 *
 * New interfaces expose to developer including {@link HCE::Mapper}, {@link HCE::Reducer},
 * {@link HCE::Combiner}, {@link HCE::Partitioner}, {@link HCE::Commiter}, {@link HCE::RecordReader}
 * and {@link HCE::RecordWriter}. These all are within C++ namespace of HCE. </p>
 * 
 * Comparing to traditional hadoop mapreduce interface, HCE has changed that Combiner can only emit
 * value. The consideration that omitting key from emit pair of combine function is due to mistaken
 * keys may corrupt the order of the map output. The output key of emit funtion is determined by the
 * input.
 *
 * And a simple word counter HCE mapreduce program lists below:
 *
 * <pre>
#include "common/hadoop/Hce.hh"
#include ""
#include "common/hadoop/StringUtils.hh"
#include "common/hadoop/SerialUtils.hh"
#include "common/hadoop/HashPartitioner.hh"

#include <string>
using std::string;

const string WORDCOUNT = "WORDCOUNT";
const string INPUT_WORDS = "INPUT_WORDS";
const string OUTPUT_WORDS = "OUTPUT_WORDS";

const int INT64_MAXLEN = 25;

class WordCountMap: public HCE::Mapper {
  public:
    HCE::TaskContext::Counter* inputWords;

    int64_t setup() {
      inputWords = getContext()->getCounter(WORDCOUNT, INPUT_WORDS);
      return 0;
    }

    int64_t map(HCE::MapInput &input) {
      int64_t valueLength;
      const void* value = input.value(valueLength);
      if(valueLength != 0) {
        std::vector<string> words =
          HadoopUtils::splitString(string((const char*)value, valueLength), " \t\r");
        for(unsigned int i=0; i < words.size(); ++i) {
          emit(words[i].c_str(), words[i].length(), "1", 1);
        }
        getContext()->incrementCounter(inputWords, words.size());
      }
      return 0;
    }

    int64_t cleanup(bool isSuccessful) {
      // Do some cleaning
      return 0;
    }
};

class WordCountReduce: public HCE::Reducer {
  public:
    HCE::TaskContext::Counter* outputWords;

    int64_t setup() {
      outputWords = getContext()->getCounter(WORDCOUNT, OUTPUT_WORDS);
      return 0;
    }

    int64_t reduce(HCE::ReduceInput &input) {
      int64_t keyLength;
      const void* key = input.key(keyLength);

      int64_t sum = 0;
      while (input.nextValue()) {
        int64_t valueLength;
        const void* value = input.value(valueLength);
        sum += HadoopUtils::toInt(string((const char*)value, valueLength));
      }

      char str[INT64_MAXLEN];
      int str_len = snprintf(str, INT64_MAXLEN, "%ld", sum);
      emit(key, keyLength, str, str_len);
      return 0;
    }

    int64_t cleanup(bool isSuccessful) {
      // Do some cleaning
      return 0;
    }
};

class WordCountCombiner: public HCE::Combiner {
  public:
    HCE::TaskContext::Counter* outputWords;

    int64_t setup() {
      // do some initialization
      return 0;
    }

    int64_t combine(HCE::ReduceInput &input) {
      int64_t sum = 0;
      while (input.nextValue()) {
        int64_t valueLength;
        const void* value = input.value(valueLength);
        sum += HadoopUtils::toInt(string((const char*)value, valueLength));
      }

      char str[INT64_MAXLEN];
      int str_len = snprintf(str, INT64_MAXLEN, "%ld", sum);
      emit(str, str_len);
      return 0;
    }

    int64_t cleanup(bool isSuccessful) {
      // Do some cleaning
      return 0;
    }
};

int main(int argc, char *argv[]) {
  return HCE::runTask(
      HCE::TemplateFactory<WordCountMap, WordCountReduce, HCE::HashPartitioner, void, void, void, void>()
      );
}
 *
 * </pre>

 */
