#ifndef __SEQUENCE_FILE_WRITER_HH
#define __SEQUENCE_FILE_WRITER_HH

#include "common/hadoop/Hce.hh"
#include "common/hadoop/TemplateFactoryHce.hh"
#include "common/hadoop/StringUtils.hh"
#include "common/hadoop/SerialUtils.hh"
#include <SequenceFile.hh>
#include <FileSystem.hh>
#include <DistributedFileSystem.hh>


namespace HCE
{
	class SequenceFileWriter
		: public RecordWriter
	{
	public:
		SequenceFileWriter();
		SequenceFileWriter(TaskContext& context);
		virtual ~SequenceFileWriter();

		virtual int64_t open() throw();
		int open(FileSystem& fs, const char* filename, SequenceFile::CompressionType compressionType, const char* codec, SequenceFile::MetaData* meta) throw();
		virtual void emit(const void* key, const uint64_t keyLength, const void* value, const uint64_t valueLength) throw();
		virtual int64_t close() throw();
		int64_t getLength() { return (_writer->getLength()); }
		/// create sync point
		void sync() throw() { _writer->sync(); }

	private:
		void init();
	private:
		SequenceFile::WriterPtr _writer;
		DistributedFileSystem* _dfs;
		SequenceFile::CompressionType _type;
		TaskContext* _context;
		std::string _fsname;
		char* _codec;
		char* _host;
		char* _file;
		char* _user;
		char* _password;

		uint16_t _port;
		bool _opened;
		bool _compress;
	};
}


#endif // __SEQUENCE_FILE_WRITER_HH

