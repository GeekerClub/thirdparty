#ifndef __SEQUENCE_FILE_READER_HH
#define __SEQUENCE_FILE_READER_HH

#include "common/hadoop/Hce.hh"
#include "common/hadoop/TemplateFactoryHce.hh"
#include "common/hadoop/StringUtils.hh"
#include "common/hadoop/SerialUtils.hh"
#include <SequenceFile.hh>
#include <FileSystem.hh>
#include <DistributedFileSystem.hh>

namespace HCE
{
	class SequenceFileReader
		: public RecordReader
	{
	public:
		SequenceFileReader();
		SequenceFileReader(TaskContext& context) throw();
		virtual ~SequenceFileReader();

		virtual int64_t open() throw();
		int open(FileSystem &fs, const char* filename) throw();
		int64_t close() throw();

		virtual int next(void*& key, int64_t& keyLength, void*& value, int64_t& valueLength) throw();
		virtual float getProgress();
		int64_t getPosition() { return (_reader->tell()); } 
		bool isCompressed() { return (_reader->isCompressed()); } 
		bool isBlockCompressed() { return (_reader->isBlockCompressed()); } 
		const SequenceFile::MetaData& getMetadata() { return (_reader->getMetadata()); }

		int seek(int64_t pos) throw() { return (_reader->seek(pos)); }
		/// Seek to the next sync mark past a given position.
		void sync(int64_t pos) throw() { _reader->sync(pos); }
		/// Returns true iff the previous call to next passed a sync mark.
		bool syncSeen()  throw() { return (_reader->syncSeen()); } 
	private:
		SequenceFile::ReaderPtr _reader;
		DistributedFileSystem* _dfs;
		BufferedOutputStream _key;
		BufferedOutputStream _value;
		TaskContext* _context;
		std::string _fsname;
		char* _host;
		char* _file;
		char* _user;
		char* _password;

		int16_t _port;
		long _startOffset;
		long _len;
		bool _opened;
	};
}


#endif // __SEQUENCE_FILE_READER_HH
