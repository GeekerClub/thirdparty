// Copyright (C) 2018, For authors.
// Author: Xiang Zhang (zhangxiang.gk@gmail.com)
//
// Description:

#ifndef PUBLIC_LIBDFS_SRC_WRAPPER_HDFS_FILESYSTEM_H
#define PUBLIC_LIBDFS_SRC_WRAPPER_HDFS_FILESYSTEM_H

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#else
typedef int bool;
#endif

#ifndef EINTERNAL
#define EINTERNAL 255
#endif

struct hdfsBuilder;
typedef int32_t     tSize;
typedef time_t      tTime;
typedef int64_t     tOffset;
typedef uint16_t    tPort;

typedef enum tObjectKind {
    kObjectKindFile = 'F',
    kObjectKindDirectory = 'D',
    kObjectKindSymlink = 'S',
} tObjectKind;

typedef enum tBlockType {
    kBlockTypeRepl = 'R',
    kBlockTypeRscode = 'S'
} tBlockType;

typedef void* hdfsFS;

/* WARNING: hdfsFile_internal is just for the sake of compatible with hdfs.h
 *          don't access attributes of hdfsFile_internal
 */
enum hdfsStreamType {
    UNINITIALIZED = 0,
    INPUT = 1,
    OUTPUT = 2,
};

struct hdfsFile_internal {
    void* file;
    enum hdfsStreamType type;
};
typedef struct hdfsFile_internal* hdfsFile;

/* WARNING: LineReader_internal is just for the sake of compatible with hdfs.h
 *          don't access attributes of LineReader_internal
 */
struct LineReader_internal {
    hdfsFile    hdfsF;
    char*       buffer;
    int         bufferSize;
    int         bufferPosn;
    int         bufferLength;
    int         eof;
    int         prevCharCR;
    tOffset     pos;
};
typedef struct LineReader_internal* LineReader;

LineReader createLineReader(hdfsFile f);
int seekLineReader(hdfsFS fs, LineReader lr, tOffset desiredPos);
tOffset getLineReaderPos(hdfsFS fs, LineReader lr);
int readLineByLineReader(hdfsFS fs, LineReader lr, void** line);
void closeLineReader(LineReader lr);

/* WARNING: SeqFile_internal is just for the sake of compatible with hdfs.h
 *          don't access attributes of SeqFile_internal
 */
struct SeqFile_internal {
    void* file;
    enum hdfsStreamType type;
    // for reading
    void* readBuf;
    int nextReadBufPos;
    int totalReadBufPos;
    // for writing
    void* writeBuf;
    int writeBufSize;
    int writeBufPos;
};

typedef struct SeqFile_internal* SeqFile;

/**
 * readSequenceFile - create sequence file for reading key/value
 *
 * @param fs The configured filesystem handle.
 * @param path the file path
 * @return Returns SeqFile handle on success; NULL on error.
 */
SeqFile readSequenceFile(hdfsFS fs, const char* path);

/**
 * seek position of Input SequenceFile
 *
 * @param f the sequence file handle
 * @param offset the new position
 * @return Returs -1, not supported
 */
int seekSeqFile(SeqFile f, tOffset offset);


/**
 * sync position of Input SequenceFile
 *
 * @param f the sequence file handle
 * @param offset the new position
 * @return Returns -1, not supported
 */
tOffset syncSeqFile(SeqFile f, tOffset offset);

/**
 * readNextRecordFromSeqFile - read next record from sequence file
 *
 * @param fs The configured filesystem handle.
 * @param f the sequence file handle
 * @param key the returned key
 * @param keyLen the returned key length
 * @param value the returned value
 * @param valueLen the returned value length
 * @return Returns 0 on success; 1 on finish; -1 on error.
 */
int readNextRecordFromSeqFile(hdfsFS fs, SeqFile f, void** key, int* keyLen,
                              void** value, int* valueLen);

/**
 * readFirstRecordFromSeqFile - read first record from sequence file
 *
 * @param fs The configured filesystem handle.
 * @param f the sequence file handle
 * @param key the returned key
 * @param keyLen the returned key length
 * @param value the returned value
 * @param valueLen the returned value length
 * @return Returns 0 on success; 1 on finish; -1 on error.
 */
int readFirstRecordFromSeqFile(hdfsFS fs, SeqFile f, void** key, int* keyLen,
                               void** value, int* valueLen);

/**
 * readLastRecordFromSeqFile - read last record from sequence file
 *
 * @param fs The configured filesystem handle.
 * @param f the sequence file handle
 * @param key the returned key
 * @param keyLen the returned key length
 * @param value the returned value
 * @param valueLen the returned value length
 * @return Returns -1, not supported
 */
int readLastRecordFromSeqFile(hdfsFS fs, SeqFile f, void** key, int* keyLen,
                              void** value, int* valueLen);

/**
 * Get current position of Output SequenceFile or Input SequenceFile
 *
 * @param file the sequence file handle
 * @return Returns >=0 on success; -1 on error.
 */
tOffset getSeqFilePos(SeqFile file);

/**
 * writeSequenceFile - create sequence file for writing
 *
 * @param fs The configured filesystem handle.
 * @param path the file path
 * @param compressionTypeString compression type: "NONE" - no compression;
 *          "BLOCK" - block compression.
 * @param codecString the compression codec: i.e. "org.apache.hadoop.io.compress.DefaultCodec"
 * @return Returns SeqFile handle on success; NULL on error.
 */
SeqFile writeSequenceFile(hdfsFS fs, const char* path, const char* compressionTypeString,
                          const char* codecString);

/**
 * appendSequenceFile - reopen an exists sequence file for writing
 *
 * @param fs The configured filesystem handle.
 * @param path the file path
 * @return Returns NULL, not supported
 */
SeqFile appendSequenceFile(hdfsFS fs, const char* path);

/**
 * writeRecordIntoSeqFile - write record into sequence file
 *
 * @param fs The configured filesystem handle.
 * @param f the sequence file handle
 * @param key the writen key
 * @param keyLen the writen key length
 * @param value the writen value
 * @param valueLen the writen value length
 * @return 0 on success; -1 on error.
 */
int writeRecordIntoSeqFile(hdfsFS fs, SeqFile f, const void* key, int keyLen,
                           const void* value, int valueLen);

/**
 * syncSeqFileFs - sync the seq file for readers can read
 *
 * @param fs The configured filesystem handle.
 * @param f the sequence file handle
 * @return 0 on success; -1 on error.
 */
int syncSeqFileFs(hdfsFS fs, SeqFile f);

/**
 * closeSequenceFile - close the sequence file
 *
 * @param fs The configured filesystem handle.
 * @param file the sequence file handle
 * @return 0 on success; -1 on error.
 */
int closeSequenceFile(hdfsFS fs, SeqFile file);

int hdfsFileIsOpenForRead(hdfsFile file);
int hdfsFileIsOpenForWrite(hdfsFile file);
hdfsFS hdfsConnectAsUser(const char *nn, tPort port, const char *user, const char *passwd);
hdfsFS hdfsConnectAsOnlyUser(const char *nn, tPort port, const char *user);
hdfsFS hdfsConnect(const char *nn, tPort port);
hdfsFS hdfsConnectAsUserNewInstance(const char *nn, tPort port, const char *user,
                                    const char *passwd);
hdfsFS hdfsConnectNewInstance(const char *nn, tPort port);
hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld);

typedef enum  { gzip = 0, bzip, lzma, lzo, quicklz} CompressType;
hdfsFile hdfsOpenFileWithDeCompress(hdfsFS fs, const char* path, int flags,
                int bufferSize, short replication, tSize blocksize, CompressType compress);

void hdfsBuilderSetForceNewInstance(struct hdfsBuilder *bld);
void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn);
void hdfsBuilderSetNameNodePort(struct hdfsBuilder *bld, tPort port);
void hdfsBuilderSetUserName(struct hdfsBuilder *bld, const char *userName);
void hdfsBuilderSetPasswd(struct hdfsBuilder *bld, const char* passwd);
void hdfsBuilderSetKerbTicketCachePath(struct hdfsBuilder *bld, const char *kerbTicketCachePath);
void hdfsFreeBuilder(struct hdfsBuilder *bld);
int hdfsBuilderConfSetStr(struct hdfsBuilder *bld, const char *key, const char *val);
struct hdfsBuilder *hdfsNewBuilder();

int hdfsDisconnect(hdfsFS fs);
hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags,
                            int bufferSize, short replication, tSize blocksize);
hdfsFile hdfsOpenFileExtra(hdfsFS fs, const char *path, int flags,
                           int bufferSize, short replication, tSize blocksize, int32_t perm,
                           bool is_append, bool is_overwrite);
int hdfsRecoverFile(hdfsFS fs, const char *path);

int hdfsCloseFile(hdfsFS fs, hdfsFile file);
int hdfsExists(hdfsFS fs, const char *path);
int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos);
tOffset hdfsTell(hdfsFS fs, hdfsFile file);
tSize hdfsRead(hdfsFS fs, hdfsFile file, void *buffer, tSize length);
tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position,
                            void *buffer, tSize length);
tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void *buffer, tSize length);

int hdfsFlush(hdfsFS fs, hdfsFile file);
int hdfsHFlush(hdfsFS fs, hdfsFile file);
int hdfsHSync(hdfsFS fs, hdfsFile file);
int hdfsSync(hdfsFS fs, hdfsFile file);
int hdfsAvailable(hdfsFS fs, hdfsFile file);

int hdfsDeleteExtra(hdfsFS fs, const char *path, int recursive);
int hdfsDelete(hdfsFS fs, const char *path);
int hdfsRename(hdfsFS fs, const char *oldPath, const char *newPath);
char* hdfsMakeQualified(hdfsFS fs, const char *path, char *buffer, size_t bufferSize);
char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize);
char* hdfsGetFileCheckSumMD5(hdfsFS fs, const char *path);

int hdfsSetWorkingDirectory(hdfsFS fs, const char *path);
int hdfsCreateDirectory(hdfsFS fs, const char *path);
int hdfsSetReplication(hdfsFS fs, const char *path, int16_t replication);

typedef struct {
    tObjectKind     mKind;
    char            *mName;
    tTime           mLastMod;
    tOffset         mSize;
    short           mReplication;
    tOffset         mBlockSize;
    char            *mOwner;
    char            *mGroup;
    short           mPermissions;
    tTime           mLastAccess;
    //tBlockType      mType;
} hdfsFileInfo;

hdfsFileInfo* hdfsListDirectory(hdfsFS fs, const char * path, int *numEntries);
hdfsFileInfo* hdfsReadDirectory(hdfsFS fs, const char * path, int *numEntries);
hdfsFileInfo *hdfsGlobStatus(hdfsFS fs, const char* path, int *numEntries);
hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path);
void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries);

struct hdfsStatus {
    tOffset capacity;
    tOffset used;
    tOffset remaining;
};

struct hdfsFileBlocksHosts {
    struct hdfsFileBlocksHosts* next;
    char            ***hosts;    // hosts
    int64_t         *lengths;
    hdfsFileInfo*   fileInfo;
    char            ***names;    // ip+port
};

char*** hdfsGetHosts(hdfsFS fs, const char *path, tOffset start, tOffset length);
void hdfsFreeHosts(char ***blockHosts);

struct hdfsFileBlocksHosts* hdfsGetFileHosts(hdfsFS fs, const char *path,
                                             tOffset start, tOffset length);
struct hdfsFileBlocksHosts* hdfsGetDirectoryHosts(hdfsFS fs, const char *path);
void hdfsFreeDirectoryHosts(struct hdfsFileBlocksHosts* blockHosts);

struct hdfsStatus* hdfsGetStatus(hdfsFS fs);
void hdfsFreeStatus(struct hdfsStatus* status);
tOffset hdfsGetDefaultBlockSize(hdfsFS fs);
tOffset hdfsGetDefaultBlockSizeAtPath(hdfsFS fs, const char *path);
tOffset hdfsGetCapacity(hdfsFS fs);
tOffset hdfsGetUsed(hdfsFS fs);
int hdfsChmod(hdfsFS fs, const char *path, short mode);
int hdfsChown(hdfsFS fs, const char *path, const char* owner, const char* group);
int hdfsUtime(hdfsFS fs, const char *path, tTime mtime, tTime atime);
int hdfsAddConf(hdfsFS fs, const char* key, const char* value);

int hdfsSymlink(hdfsFS fs, const char *old_path, const char *new_path);
int hdfsLink(hdfsFS fs, const char *old_path, const char *new_path);
int hdfsTruncate(hdfsFS fs, const char *path, int64_t truncate_size);
int hdfsCreate(hdfsFS fs, const char *path, int32_t perm, int32_t replication, int32_t block_size);
int hdfsCreateExtra(hdfsFS fs, const char *path, int32_t perm, int32_t replication,
                    int32_t block_size, int is_append, int is_overwrite);
int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);
int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);

const char* hdfsGetErrMessage();
const char* hdfsGetNetErrMessage();

void setComlog(const char *dir, const char *filename);

#ifdef UNIT_TEST
extern void clear_hdfs_cache();
#endif

#ifdef __cplusplus
}
#endif

#endif  //__LIBDFS_HDFS_FILESYSTEM_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 et: */
