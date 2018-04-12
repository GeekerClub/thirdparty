#ifndef PUBLIC_LIBDFS_SRC_WRAPPER_AFS_FILESYSTEM_H
#define PUBLIC_LIBDFS_SRC_WRAPPER_AFS_FILESYSTEM_H

#include <sys/stat.h>
#include <sys/types.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <string>
#include <vector>
#include <map>

namespace dfs {

int GetErrno();
int GetRc();
void Perror(const char* s = NULL);

enum DataCodecType {
    kNoCompress,
    kGzip,
    kLzo,
    kLzma,
    kLz4,
    kInvalidCodec,
};

enum StripeType {
    kReplica, ///< replica block
    kRSCode, ///< Reed-solomon code
    kInvalidStripe,
};

static const char* const stripe_type_str[] = {
    "replica",
    "rscode",
    "invalid_stripe",
};

static inline const char* StripeType2Str(const int type) {
    if (type >= 0 &&
        type < static_cast<int>(sizeof(stripe_type_str) / sizeof(stripe_type_str[0]))) {
        return stripe_type_str[type];
    } else {
        return "unknown_stripe_type";
    }
}

static const int kMediaTypeNum = 3;

enum MediaType {           // 2 bit in inode
    MT_SSD = 0,
    MT_DISK = 1,
    MT_MEMORY = 2,
    MT_INVALID = 3,
};

static const char* const media_type_str[] = {
    "ssd",
    "disk",
    "memory",
    "invalid",
};

static inline const char* MediaType2Str(const int type) {
    if (type < 0 ||
            type >= static_cast<int>(sizeof(media_type_str) / sizeof(media_type_str[0]))) {
        return "unknown_media_type";
    } else {
        return media_type_str[type];
    }
}

static inline MediaType GetMediaTypeFromInode(const uint64_t id) {
    return static_cast<MediaType>((id >> 47) & 3);
}

enum FileType {            // 2 bit in inode
    FT_REGULAR = 0,
    FT_DIRECTORY = 1,
    FT_SYMLINK = 2,
};

static const char* const file_type_str[] = {
    "regular",
    "directory",
    "symlink",
};

static inline const char* FileType2Str(const int type) {
    if (type < 0 ||
            type >= static_cast<int>(sizeof(file_type_str) / sizeof(file_type_str[0]))) {
        return "unknown_file_type";
    } else {
        return file_type_str[type];
    }
}

static inline FileType GetFileTypeFromInode(const uint64_t id) {
    return static_cast<FileType>((id >> 49) & 3);
}

enum ReplicaWriteMode {
    kPipeLine,
    kDispatch,
    kInvalidWriteMode,
};

struct RS {
    uint16_t s; // split count
    uint8_t c; // code part count;
    uint8_t lc; // local code part count;
};

// compact with espnode & espaddr
#pragma pack(push, r1, 1)
union BlockAddr {
    uint64_t node;
    struct {
        uint16_t stub;
        uint16_t port;
        uint32_t ip;
    };

    BlockAddr(uint64_t in = 0): node(in) {}
    uint64_t Node() const {
        return node;
    }
};
#pragma pack(pop, r1)

struct BlockAttr {
    uint64_t blockid;
    uint64_t offset;
    uint32_t blocksize;
    uint32_t realsize;
    uint32_t idx;
};

struct BlockLocation {
    BlockAttr attr;
    std::vector<BlockAddr> location;
};

enum BlockSizeType {
    BST_1M = 1,
    BST_2M = 2,
    BST_4M = 4,
    BST_8M = 8,
    BST_16M = 16,
    BST_32M = 32,
    BST_64M = 64,
    BST_256M =  256,
};

static inline uint32_t BlockSizeType2Size(BlockSizeType type) {
    switch (type) {
    case BST_1M: return 1 * 1024 * 1024;
    case BST_2M: return 2 * 1024 * 1024;
    case BST_4M: return 4 * 1024 * 1024;
    case BST_8M: return 8 * 1024 * 1024;
    case BST_16M: return 16 * 1024 * 1024;
    case BST_32M: return 32 * 1024 * 1024;
    case BST_64M: return 64 * 1024 * 1024;
    case BST_256M: return 256 * 1024 * 1024;
    default: return 0;
    }
}

static inline BlockSizeType Size2BlockSizeType(uint32_t size) {
    switch (size) {
    case 1 * 1024 * 1024 : return BST_1M;
    case 2 * 1024 * 1024 : return BST_2M;
    case 4 * 1024 * 1024 : return BST_4M;
    case 8 * 1024 * 1024 : return BST_8M;
    case 16 * 1024 * 1024 : return BST_16M;
    case 32 * 1024 * 1024 : return BST_32M;
    case 64 * 1024 * 1024 : return BST_64M;
    case 256 * 1024 * 1024 : return BST_256M;
    default: return BST_256M;
    }
}

static inline const char* BlockSizeType2Str(const int type) {
    switch (type) {
    case BST_1M:   return "1M";
    case BST_2M:   return "2M";
    case BST_4M:   return "4M";
    case BST_8M:   return "8M";
    case BST_16M:  return "16M";
    case BST_32M:  return "32M";
    case BST_64M:  return "64M";
    case BST_256M: return "256M";
    default: {
        return "0M";
    }
    }
}

struct DirEntry {
    uint64_t inode;
    std::string name;
};

struct FsStatInfo {
    uint64_t disk_quota;
    uint64_t disk_used;
    uint64_t disk_inode;
    uint64_t ssd_quota;
    uint64_t ssd_used;
    uint64_t ssd_inode;
    uint64_t num_symlink;
    uint64_t num_dir;

    FsStatInfo() :
            disk_quota(UINT64_MAX),
            disk_used(UINT64_MAX),
            disk_inode(UINT64_MAX),
            ssd_quota(UINT64_MAX),
            ssd_used(UINT64_MAX),
            ssd_inode(UINT64_MAX),
            num_symlink(UINT64_MAX),
            num_dir(UINT64_MAX) {
    }
};

enum TrashStrategy {
    CONF_TRASH = 0,
    USE_TRASH,
    NO_TRASH,
};

static const int AFSReturnCodeStart = -1024;
enum ReturnCode {
    kOk = 0,

    kFail = AFSReturnCodeStart,
    kExist,
    kNoEntry,
    kPermission,
    kNotEmpty,
    kInvalidMessage,
    kInvalidParam,
    kInvalid,
    kTimeout,
    kIsDir,
    kNotDir,
    kNoSpace,
    kNameTooLong,
    kNotSupport,
    kBusy,
};

static inline int Rc2Errno(int rc) {
    switch (rc) {
    case kOk:
        return 0;
    case kExist:
        return EEXIST;
    case kNoEntry:
        return ENOENT;
    case kPermission:
        return EPERM;
    case kInvalidMessage:
        return EIO;
    case kInvalid:
        return EINVAL;
    case kTimeout:
        return EIO;
    case kIsDir:
        return EISDIR;
    case kNotDir:
        return ENOTDIR;
    case kNoSpace:
        return ENOSPC;
    case kNameTooLong:
        return ENAMETOOLONG;
    case kNotSupport:
        return ENOTSUP;
    case kBusy:
        return EBUSY;
    default:
        return EIO;
    }
}

static const char* const rc_message[] = {
    "fail",
    "exist",
    "no_entry",
    "permission",
    "not_empty",
    "invalid_message",
    "invalid_param",
    "invalid",
    "timeout",
    "is_dir",
    "not_dir",
    "no_space",
    "name_too_long",
    "not_support",
    "busy",
};

static inline const char* Rc2Str(const int rc) {
    if (rc < AFSReturnCodeStart || rc > 0) {
        return "unknown_return_code";
    } else if (rc == 0) {
        return "ok";
    }

    int idx = rc - AFSReturnCodeStart;
    if (idx >= static_cast<int>(sizeof(rc_message) / sizeof(rc_message[0]))) {
        return "unknown_return_code";
    } else {
        return rc_message[idx];
    }
}

///////////////////////////////////////////////
//
enum PacketSizeType {
    PST_16K = 16,
    PST_32K= 32,
    PST_64K = 64,
    PST_128K = 128,
    PST_256K = 256,
    PST_512K = 512,
    PST_1M = 1024,
    PST_2M =  2048,
    PST_4M = 4096
};

struct CreateOptions {
    MediaType media_type;   // media type, memory, ssd or disk
    StripeType stripe_type; // stripe type, kReplica or kRSCode, default kReplica
    BlockSizeType block_size_type; // block size, default 256M
    int num_replica; // replica num when stripe_type=kReplica, default 3
    int rs_stripe_size; // stripe size when Reed-solomon code, default 1M
    int rs_data_stripe; // Reed-solomon code's data stripe num, default 6
    int rs_code_stripe; // Reed-solomon code's code stripe num, default 3
    int rs_lcode_stripe; // Reed-solomon local code's code stripe num, default 2
    bool overwrite;      // overwrite when dist exist

    CreateOptions() :
        media_type(MT_DISK),
        stripe_type(kReplica),
        block_size_type(BST_256M),
        num_replica(0),
        rs_stripe_size(1 * 1024 * 1024),
        rs_data_stripe(6),
        rs_code_stripe(3),
        rs_lcode_stripe(2),
        overwrite(false) {}
};

struct WriterOptions {
    ReplicaWriteMode write_mode; // write mode: kPipeLine or kDispatch, default kPipeLine
    int num_safe_replica;   // safe replica num, default 1
    int num_quorum;  // write quorum, to accelerate write, default 3 (MUST num_quorum <= m_safe_replica)
    int num_alloc_replica;  // valid when kDispatch, the replica num alloced
    uint32_t time_in_mem_s;   // seconds that stay in memory, default 0

    uint32_t buffer_size;   // append buffer size, default UINT32_MAX(read from conf file)
    uint32_t total_timeout_ms; // append total timeout, default UINT32_MAX(read from conf file)
    uint32_t retry_times; // append retry times, default UINT32_MAX(read from conf file)
    uint32_t req_timeout_ms; // append request timeout, default UINT32_MAX(read from conf file)
    uint32_t slow_node_timeout_ms; // append slow node timeout, default UINT32_MAX(read from conf file)
    bool choose_local;
    DataCodecType transmit_codec_type; // compressed data on transmit, lz4 is recommended
    DataCodecType compress_codec_type; // compressed data on append
    int num_memory_replica;

    WriterOptions() :
        write_mode(kPipeLine),
        num_safe_replica(2),
        num_quorum(2),
        num_alloc_replica(3),
        time_in_mem_s(0),

        buffer_size(UINT32_MAX),
        total_timeout_ms(UINT32_MAX),
        retry_times(UINT32_MAX),
        req_timeout_ms(UINT32_MAX),
        slow_node_timeout_ms(UINT32_MAX),
        choose_local(true),
        transmit_codec_type(kNoCompress),
        compress_codec_type(kNoCompress),
        num_memory_replica(0) {}
};

struct ReaderOptions {
    uint32_t buffer_size;   // read buffer size, default UINT32_MAX(read from conf file)
    uint32_t total_timeout_ms; // read total timeout, default UINT32_MAX(read from conf file)
    uint32_t retry_times; // read retry times, default UINT32_MAX(read from conf file)
    uint32_t req_timeout_ms; // read request timeout, default UINT32_MAX(read from conf file)
    PacketSizeType packet_size_type; // read request packet size

    ReaderOptions() :
        buffer_size(UINT32_MAX),
        total_timeout_ms(UINT32_MAX),
        retry_times(UINT32_MAX),
        req_timeout_ms(UINT32_MAX),
        packet_size_type(PST_128K) {}
};

class WriterImpl;
class Writer {
public:
    Writer(WriterImpl* writer_impl) : m_writer_impl(writer_impl) {}
    virtual ~Writer();

    /**
     * @brief append data to afs
     *
     * @param buf input buf
     * @param count need append len
     *
     * @return   >=0: real write len
     *            <0: fail
     */
    int Append(const void* buf, uint32_t count);

    /**
     * @brief get appendstream current position
     *
     * @return   >=0: current position
     *           <0: fail
     */
    int64_t Tell();

    /**
     * @brief flush client's data, NOT update file size
     *
     * @return   0: success
     *          <0: fail
     */
    int Flush();

    /**
     * @brief flush client's data, update file size, then others can read
     *
     * @return   0: success
     *          <0: fail
     */
    int Sync();

    /**
     * @brief get file stat like fstat
     *
     * @param stat_buf output stat struct, only support st_ino, st_size,
     * st_mtime, st_atime
     *
     * @return   0: success
     *          <0: fail
     */
    int Stat(struct stat* stat_buf);

private:
    /**
     * @brief close writer
     *
     * @return     0: success
     *            <0: fail
     */
    int Close();

    friend class AfsFileSystem;
    WriterImpl* m_writer_impl;
};

class ReaderImpl;
class Reader {
public:
    Reader(ReaderImpl* reader_impl) : m_reader_impl(reader_impl) {}
    virtual ~Reader();

    /**
     * @brief read from readstream
     *
     * @param buf output buf
     * @param count need read len
     *
     * @return   >0: read output len, if ret_len < count read eof
     *           =0: read eof
     *           <0: read fail
     *
     * @note refresh file size when eof
     */
    int Read(void* buf, uint32_t count);

    /**
     * @brief read from specific offset
     *
     * @param buf output buf
     * @param count need read len
     * @param offset need read offset
     *
     * @return   >0: read output len, if ret_len < count read eof
     *           =0: read eof
     *           <0: read fail
     */
    int PRead(void* buf, uint32_t count, uint64_t offset);

    /**
     * @brief seek to specific offset
     *
     * @param offset input offset
     */
    void Seek(uint64_t offset);

    /**
     * @brief get readstream current position
     *
     * @return   >=0: success, current position
     *            <0: fail
     */
    int64_t Tell();

    /**
     * @brief number of bytes that can be read from this file
     *
     * @return   >=0: success, number of bytes can be read
     *            <0: fail
     *
     * @note NOT refresh file size when eof, Read() will refresh file size
     */
    int64_t Available();

    /**
     * @brief get file stat like fstat
     *
     * @param stat_buf output stat struct, only support st_ino, st_size,
     * st_mtime, st_atime
     *
     * @return   0: success
     *          <0: fail
     */
    int Stat(struct stat* stat_buf);

private:
    /**
     * @brief close reader
     *
     * @return     0: success
     *            <0: fail
     */
    int Close();

    friend class AfsFileSystem;
    ReaderImpl* m_reader_impl;
};

class AFSImpl;
class AfsFileSystem {
public:
    /**
     * @brief AfsFileSystem constructor
     *
     * @param uri afs master addr, ip:port
     * @param user afs user name
     * @param passwd afs user password
     * @param config_file afs config path
     */
    AfsFileSystem(const char* uri,
                  const char* user,
                  const char* passwd,
                  const char* config_file);

    /**
     * @brief AfsFileSystem destructor
     */
    virtual ~AfsFileSystem();

    /**
     * @brief init kylin and comlog
     *
     * @param init_kylin kylin init flag
     * @param init_log comlog init flag
     *
     * @return   0: success
     *          <0: fail
     */
    int Init(bool init_kylin = false, bool init_log = false);

    /**
     * @brief set config
     *
     * @param key config key
     * @param val config value
     *
     * @return   0: success
     *          <0: fail
     */
    int SetConfigStr(const char *key, const char *val);

    /**
     * @brief load hadoop xml config
     *
     * @param class_path CLASS_PATH, internal append "/conf/hadoop-default.xml"
     *
     * @return   0: success
     *          <0: fail
     */
    int LoadHadoopConf(const char* class_path);

    /**
     * @brief connect afs filesystem
     *
     * @return   0: success
     *          <0: fail
     */
    int Connect();

    /**
     * @brief disconnect from afs filesystem
     *
     * @return   0: success
     *          <0: fail
     */
    int DisConnect();

    /**
     * @brief get working directory
     *
     * @param buffer working directory path buffer
     * @param buffer_size working directory path buffer_size
     *
     * @return   0: success
     *          <0: fail
     */
    int GetWorkingDirectory(char *buffer, size_t buffer_size);

     /**
     * @brief set working directory
     *
     * @param path working directory path
     *
     * @return   0: success
     *          <0: fail
     */
    int SetWorkingDirectory(const char *path);

    /**
     * @brief create new file
     *
     * @param path new file path
     * @param options create options, use default options when NULL
     *
     * @return   0: success
     *          <0: fail
     */
    int Create(const char* path, const CreateOptions& options = CreateOptions());

    /**
     * @brief open writer
     *
     * @param path file path
     * @param options writer options, use default options
     *
     * @return   not null: success
     *               null: fail
     */
    Writer* OpenWriter(const char* path, const WriterOptions& options = WriterOptions());

    /**
     * @brief close writer
     *
     * @param writer input writer
     *
     * @return   0: success
     *          <0: fail
     */
    int CloseWriter(Writer* writer);

    /**
     * @brief open reader
     *
     * @param path file path
     * @param options reader options, use default options
     *
     * @return   not null: success
     *               null: fail
     */
    Reader* OpenReader(const char* path, const ReaderOptions& options = ReaderOptions());

    /**
     * @brief close reader
     *
     * @param reader input reader
     *
     * @return   0: success
     *          <0: fail
     */
    int CloseReader(Reader* reader);

    /**
     * @brief truncate file
     *
     * @param path file path
     * @param length trucate new file length
     *
     * @return   0: success
     *          <0: fail
     */
    int Truncate(const char* path, uint64_t length);

    /**
     * @brief truncate to last sync
     *
     * @param path input file path
     *
     * @return   0: success
     *          <0: fail
     */
    int TruncateLastSync(const char* path);

    /**
     * @brief truncate to last block's shortest length
     *
     * @param path input file path
     *
     * @return   0: success
     *          <0: fail
     */
    int TruncateLease(const char* path);

    /**
     * @brief make new directory, like mkdir -p
     *
     * @param path new dir path
     *
     * @return   0: success
     *          <0: fail
     */
    int Mkdir(const char* path);

    /**
     * @brief make new directory, like mkdir -p, if dir exist return kExist
     *
     * @param path new dir path
     *
     * @return   0: success
     *          <0: fail
     */
    int CheckAndMkdir(const char* path);

    /**
     * @brief Delete file or dir
     *
     * @param path file or dir patch
     * @param recursively can delete NO_EMPTY dir when recursively is TRUE
     * @param strategy set trash strategy
     *
     * @return   0: success
     *          <0: fail
     */
    int Delete(const char* path, bool recursively = false, TrashStrategy strategy = CONF_TRASH);

    /**
     * @brief create hard link to file
     *
     * @param old_path old file path
     * @param new_path new hard link path
     *
     * @return   0: success
     *          <0: fail
     */
    int Link(const char* old_path, const char* new_path);

    /**
     * @brief create symlink to file or dir or symlink
     *
     * @param old_path old dentry path
     * @param new_path new symlink path
     *
     * @return   0: success
     *          <0: fail
     */
    int Symlink(const char* old_path, const char* new_path);

    /**
     * @brief rename old path to new path
     *
     * @param old_path input old path
     * @param new_path input new path
     *
     * @return   0: success
     *          <0: fail
     */
    int Rename(const char* old_path, const char* new_path);

    /**
     * @brief move old path to new path
     *
     * @param old_path input old path
     * @param new_path input new path
     *
     * @return   0: success
     *          <0: fail
     */
    int Move(const char* old_path, const char* new_path);

    /**
     * @brief read child entry from dir
     *
     * @param path input dir path
     * @param dirents output child entries
     *
     * @return   0: success
     *          <0: fail
     */
    int Readdir(const char* path, std::vector<DirEntry>* dirents);

    /**
     * @brief get dentry stat
     *
     * @param path input dentry path
     * @param stat_buf output stat struct, only support st_ino, st_size,
     * st_mtime, st_atime and S_IFREG, S_IFDIR, S_IFLNK of st_mode
     *
     * @return   0: success
     *          <0: fail
     */
    int Stat(const char* path, struct stat* stat_buf);

    /**
     * @brief check path's exist
     *
     * @param path input dentry path
     *
     * @return   0: exist
     *           <0: not exist, or no permission
     */
    int Exist(const char* path);

    /**
     * @brief check path's permission
     *
     * @param path input dentry path
     * @param mode check mode, F_OK/R_OK/W_OK, F_OK check read permission and exist,
     *             other check permission
     *
     * @return   0: success
     *          <0: fail
     */
    int Access(const char* path, int mode);

    /**
     * @brief modify dentry mtime or atime
     *
     * @param path input dentry path
     * @param mtime modify mtime if mtime != UINT64_MAX
     * @param atime modify atime if atime != UINT64_MAX
     *
     * @return   0: success
     *          <0: fail
     */
    int Utime(const char* path, time_t mtime, time_t atime);

    /**
     * @brief set file's replica num, ONLY for inode's StripeType==kReplica
     *
     * @param path input file path
     * @param replica modify replica num
     *
     * @return   0: success
     *          <0: fail
     */
    int SetReplica(const char* path, int16_t replica);

    /**
     * @brief get file's block location
     *
     * @param path input file path
     * @param offset input file offset
     * @param length input file length
     * @param locations output block location
     *
     * @return   0: success
     *          <0: fail
     */
    int GetBlockLocation(const char* path,
                         uint64_t offset,
                         uint64_t length,
                         std::vector<BlockLocation>* locations);

    /**
     * @brief get filesystem stat info
     *
     * @param fs_info output filesystem stat info
     *
     * @return   0: success
     *          <0: fail
     */
    int StatFs(FsStatInfo* fs_info);

private:
    AFSImpl* m_impl;
};

class AfsFileSystemCache {
public:
    AfsFileSystemCache() {
        pthread_mutex_init(&_lock, NULL);
    }
    ~AfsFileSystemCache() {
        //destroy();
    }

    /**
     * @brief get filesystem instance by uri|user|password|config_string
     *
     * @param uri filesystem uri, like afs://xxx:1234 or hdfs://xxx:1234
     * @param user user
     * @param password password, giano set, giano
     * @param class_path local path like HADOOP_CLASSPATH
     * @param config_string extra config key-value string
     *
     * @return   filesystem ptr
     */
    AfsFileSystem* get(const char* uri, const char* user,
                       const char* password, const char* class_path,
                       const std::map<std::string, std::string>& config_string);

    /**
     * @brief destroy cached filesystem instance
     */
    void destroy();
private:
    AfsFileSystem* connect(AfsFileSystem* fs, uint64_t key);

    std::map<uint64_t, AfsFileSystem*> _cache;
    pthread_mutex_t _lock;
};

} // end namespace dfs

#endif
