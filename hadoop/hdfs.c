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

#include <time.h>
#include <rpc/types.h>
#include <rpc/xdr.h>
#include <string.h>

#include "hdfs.h"
#include "hdfsJniHelper.h"

/* Some frequently used Java paths */
#define HADOOP_CONF     "org/apache/hadoop/conf/Configuration"
#define HADOOP_PATH     "org/apache/hadoop/fs/Path"
#define HADOOP_LOCALFS  "org/apache/hadoop/fs/LocalFileSystem"
#define HADOOP_FS       "org/apache/hadoop/fs/FileSystem"
#define HADOOP_BLK_LOC  "org/apache/hadoop/fs/BlockLocation"
#define HADOOP_DFS      "org/apache/hadoop/hdfs/DistributedFileSystem"
#define HADOOP_ISTRM    "org/apache/hadoop/fs/FSDataInputStream"
#define HADOOP_OSTRM    "org/apache/hadoop/fs/FSDataOutputStream"
#define HADOOP_STAT     "org/apache/hadoop/fs/FileStatus"
#define HADOOP_FSPERM   "org/apache/hadoop/fs/permission/FsPermission"
#define HADOOP_UNIX_USER_GROUP_INFO "org/apache/hadoop/security/UnixUserGroupInformation"
#define HADOOP_USER_GROUP_INFO "org/apache/hadoop/security/UserGroupInformation"
#define HADOOP_JOBCLIENT "org/apache/hadoop/mapred/JobClient"
#define HADOOP_JOBSTATUS "org/apache/hadoop/mapred/JobStatus"
#define HADOOP_SEQUENCEFILE_READER "org/apache/hadoop/mapred/SequenceFileReader"
#define HADOOP_SEQUENCEFILE_WRITER "org/apache/hadoop/mapred/SequenceFileWriter"
#define HADOOP_SEQUENCEFILE_READER2 "org/apache/hadoop/mapred/NewSequenceFileReader"
#define HADOOP_SEQUENCEFILE_WRITER2 "org/apache/hadoop/mapred/NewSequenceFileWriter"
#define JAVA_NET_ISA    "java/net/InetSocketAddress"
#define JAVA_NET_URI    "java/net/URI"
#define JAVA_STRING     "java/lang/String"
#define JAVA_ISTRM     "java/io/InputStream"

#define JAVA_VOID       "V"

/* Macros for constructing method signatures */
#define JPARAM(X)           "L" X ";"
#define JARRPARAM(X)        "[L" X ";"
#define JMETHOD1(X, R)      "(" X ")" R
#define JMETHOD2(X, Y, R)   "(" X Y ")" R
#define JMETHOD3(X, Y, Z, R)   "(" X Y Z")" R
#define JMETHOD4(X, Y, Z, P, R)   "(" X Y Z P")" R

/**
 * hdfsJniEnv: A wrapper struct to be used as 'value'
 * while saving thread -> JNIEnv* mappings
 */
typedef struct
{
    JNIEnv* env;
} hdfsJniEnv;

void endian_swap(unsigned int* x)
{
    *x = ((*x)>>24) |
        (((*x)<<8) & 0x00FF0000) |
        (((*x)>>8) & 0x0000FF00) |
        ((*x)<<24);
}

/**
 * Helper function to destroy a local reference of java.lang.Object
 * @param env: The JNIEnv pointer.
 * @param jFile: The local reference of java.lang.Object object
 * @return None.
 */
static void destroyLocalReference(JNIEnv *env, jobject jObject)
{
  if (jObject)
    (*env)->DeleteLocalRef(env, jObject);
}


/**
 * Helper function to create a org.apache.hadoop.fs.Path object.
 * @param env: The JNIEnv pointer.
 * @param path: The file-path for which to construct org.apache.hadoop.fs.Path
 * object.
 * @return Returns a jobject on success and NULL on error.
 */
static jobject constructNewObjectOfPath(JNIEnv *env, const char *path)
{
    //Construct a java.lang.String object
    jstring jPathString = (*env)->NewStringUTF(env, path);

    //Construct the org.apache.hadoop.fs.Path object
    jobject jPath =
        constructNewObjectOfClass(env, NULL, "org/apache/hadoop/fs/Path",
                                  "(Ljava/lang/String;)V", jPathString);
    if (jPath == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.fs.Path for %s\n", path);
        errno = EINTERNAL;
        return NULL;
    }

    // Destroy the local reference to the java.lang.String object
    destroyLocalReference(env, jPathString);

    return jPath;
}


/**
 * Helper function to translate an exception into a meaningful errno value.
 * @param exc: The exception.
 * @param env: The JNIEnv Pointer.
 * @param method: The name of the method that threw the exception. This
 * may be format string to be used in conjuction with additional arguments.
 * @return Returns a meaningful errno value if possible, or EINTERNAL if not.
 */
static int errnoFromException(jthrowable exc, JNIEnv *env,
                              const char *method, ...)
{
    va_list ap;
    int errnum = 0;
    char *excClass = NULL;

    if (exc == NULL)
        goto default_error;

    if ((excClass = classNameOfObject((jobject) exc, env)) == NULL) {
      errnum = EINTERNAL;
      goto done;
    }

    if (!strcmp(excClass, "org.apache.hadoop.fs.permission."
                "AccessControlException")) {
        errnum = EACCES;
        goto done;
    }

    //TODO: interpret more exceptions; maybe examine exc.getMessage()

default_error:

    //Can't tell what went wrong, so just punt
    (*env)->ExceptionDescribe(env);
    fprintf(stderr, "Call to ");
    va_start(ap, method);
    vfprintf(stderr, method, ap);
    va_end(ap);
    fprintf(stderr, " failed!\n");
    errnum = EINTERNAL;

done:

    (*env)->ExceptionClear(env);

    if (excClass != NULL)
        free(excClass);

    return errnum;
}




hdfsFS hdfsConnect(const char* host, tPort port) {
  // conect with NULL as user name/groups
  return hdfsConnectAsUser(host, port, NULL, NULL);
}


hdfsFS hdfsConnectAsUser(const char* host, tPort port, const char *user , const char *password)
{
    // JAVA EQUIVALENT:
    //  FileSystem fs = FileSystem.get(new Configuration());
    //  return fs;

    JNIEnv *env = 0;
    jobject jConfiguration = NULL;
    jobject jFS = NULL;
    jobject jURI = NULL;
    jstring jURIString = NULL;
    jvalue  jVal;
    jthrowable jExc = NULL;
    char    *cURI = 0;
    jobject gFsRef = NULL;


    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    //Create the org.apache.hadoop.conf.Configuration object
    jConfiguration =
        constructNewObjectOfClass(env, NULL, HADOOP_CONF, "()V");

    if (jConfiguration == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        return NULL;
    }

    if (user != NULL) {

      if (password == NULL) {
        fprintf(stderr, "ERROR: password must not be empty/null\n");
        errno = EINVAL;
        return NULL;
      }

      jstring jUserString = (*env)->NewStringUTF(env, user);
      jstring jPasswordString = (*env)->NewStringUTF(env, password);

      jobject jUgi;
      if ((jUgi = constructNewObjectOfClass(env, &jExc, HADOOP_UNIX_USER_GROUP_INFO, JMETHOD2(JPARAM(JAVA_STRING), JPARAM(JAVA_STRING), JAVA_VOID), jUserString, jPasswordString)) == NULL) {
        fprintf(stderr,"failed to construct hadoop user info object\n");
        errno = errnoFromException(jExc, env, HADOOP_UNIX_USER_GROUP_INFO,
                                   "init");
        destroyLocalReference(env, jConfiguration);
        destroyLocalReference(env, jUserString);
        destroyLocalReference(env, jPasswordString);
        return NULL;
      }
#define USE_UUGI
#ifdef USE_UUGI

      // UnixUserGroupInformation.UGI_PROPERTY_NAME
      jstring jAttrString = (*env)->NewStringUTF(env,"hadoop.job.ugi");

      if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, HADOOP_UNIX_USER_GROUP_INFO, "saveToConf",
                       JMETHOD3(JPARAM(HADOOP_CONF), JPARAM(JAVA_STRING), JPARAM(HADOOP_UNIX_USER_GROUP_INFO), JAVA_VOID),
                       jConfiguration, jAttrString, jUgi) != 0) {
        errno = errnoFromException(jExc, env, HADOOP_FSPERM,
                                   "init");
        destroyLocalReference(env, jConfiguration);
        destroyLocalReference(env, jUserString);
        destroyLocalReference(env, jPasswordString);
        destroyLocalReference(env, jUgi);
        return NULL;
      }

      destroyLocalReference(env, jUserString);
      destroyLocalReference(env, jPasswordString);
      destroyLocalReference(env, jUgi);
    }
#else

    // what does "current" mean in the context of libhdfs ? does it mean for the last hdfs connection we used?
    // that's why this code cannot be activated. We know the above use of the conf object should work well with
    // multiple connections.
      if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, HADOOP_USER_GROUP_INFO, "setCurrentUGI",
                       JMETHOD1(JPARAM(HADOOP_USER_GROUP_INFO), JAVA_VOID),
                       jUgi) != 0) {
        errno = errnoFromException(jExc, env, HADOOP_USER_GROUP_INFO,
                                   "setCurrentUGI");
        destroyLocalReference(env, jConfiguration);
        destroyLocalReference(env, jUserString);
        destroyLocalReference(env, jPasswordString);
        destroyLocalReference(env, jUgi);
        return NULL;
      }

      destroyLocalReference(env, jUserString);
      destroyLocalReference(env, jPasswordString);
      destroyLocalReference(env, jUgi);
    }
#endif
    //Check what type of FileSystem the caller wants...
    if (host == NULL) {
        // fs = FileSytem::getLocal(conf);
        if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, HADOOP_FS, "getLocal",
                         JMETHOD1(JPARAM(HADOOP_CONF),
                                  JPARAM(HADOOP_LOCALFS)),
                         jConfiguration) != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "FileSystem::getLocal");
            goto done;
        }
        jFS = jVal.l;
    }
    else if (!strcmp(host, "default") && port == 0) {
        //fs = FileSystem::get(conf);
        if (invokeMethod(env, &jVal, &jExc, STATIC, NULL,
                         HADOOP_FS, "get",
                         JMETHOD1(JPARAM(HADOOP_CONF),
                                  JPARAM(HADOOP_FS)),
                         jConfiguration) != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "FileSystem::get");
            goto done;
        }
        jFS = jVal.l;
    }
    else {
        // fs = FileSystem::get(URI, conf);
        cURI = malloc(strlen(host)+16);
        if (cURI == NULL) {
          errno = ENOMEM;
          goto done;
        }
        sprintf(cURI, "hdfs://%s:%d", host, (int)(port));

        jURIString = (*env)->NewStringUTF(env, cURI);
        if (jURIString == NULL) {
          errno = ENOMEM;
          goto done;
        }

        if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, JAVA_NET_URI,
                         "create", "(Ljava/lang/String;)Ljava/net/URI;",
                         jURIString) != 0) {
            errno = errnoFromException(jExc, env, "java.net.URI::create");
            goto done;
        }
        jURI = jVal.l;

        if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, HADOOP_FS, "get",
                         JMETHOD2(JPARAM(JAVA_NET_URI),
                                  JPARAM(HADOOP_CONF), JPARAM(HADOOP_FS)),
                         jURI, jConfiguration) != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "Filesystem::get(URI, Configuration)");
            goto done;
        }

        jFS = jVal.l;
    }

  done:

    // Release unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jURIString);
    destroyLocalReference(env, jURI);

    if (cURI) free(cURI);

    /* Create a global reference for this fs */
    if (jFS) {
        gFsRef = (*env)->NewGlobalRef(env, jFS);
        destroyLocalReference(env, jFS);
    }

    return gFsRef;
}



int hdfsDisconnect(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.close()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
      errno = EINTERNAL;
      return -2;
    }

    //Parameters
    jobject jFS = (jobject)fs;

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (fs == NULL) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "close", "()V") != 0) {
        errno = errnoFromException(jExc, env, "Filesystem::close");
        return -1;
    }

    //Release unnecessary references
    (*env)->DeleteGlobalRef(env, fs);

    return 0;
}

static hdfsFile _hdfsOpen(hdfsFS fs, const char *path, int flags,
         int bufferSize, short replication, tSize blockSize, int codec)
{
    if (bufferSize < 0){
       errno = EINVAL;
       return NULL;
    }

    /* Get the JNIEnv* corresponding to current thread */
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    if (flags & O_RDWR) {
      fprintf(stderr, "ERROR: cannot open an hdfs file in O_RDWR mode\n");
      errno = ENOTSUP;
      return NULL;
    }

    if ((flags & O_CREAT) && (flags & O_EXCL)) {
        fprintf(stderr, "WARN: hdfs does not truly support O_CREATE && O_EXCL\n");
    }

    /* The hadoop java api/signature */
    const char* method;
    const char* signature;

    if((flags & O_WRONLY) == 0)
    {
        if(codec == -1)
        {
            method = "open";
            signature = JMETHOD2(JPARAM(HADOOP_PATH), "I", JPARAM(HADOOP_ISTRM));
        }
        else
        {
            method = "openWithCodec";
            signature = JMETHOD2(JPARAM(HADOOP_PATH), "II", JPARAM(JAVA_ISTRM));
        }
    }
    else if((flags & O_APPEND))
    {
        method = "append";
        signature = JMETHOD1(JPARAM(HADOOP_PATH), JPARAM(HADOOP_OSTRM));
    }
    else
    {
        method = "create";
        signature = JMETHOD2(JPARAM(HADOOP_PATH), "ZISJ", JPARAM(HADOOP_OSTRM));
    }



    /* Return value */
    hdfsFile file = NULL;

    /* Create an object of org.apache.hadoop.fs.Path */
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    /* Get the Configuration object from the FileSystem object */
    jvalue  jVal;
    jobject jConfiguration = NULL;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "getConf", JMETHOD1("", JPARAM(HADOOP_CONF))) != 0) {
        errno = errnoFromException(jExc, env, "get configuration object "
                                   "from filesystem");
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jConfiguration = jVal.l;

    jint jBufferSize = bufferSize;
    jshort jReplication = replication;
    jint jCodec = codec;
    jlong jBlockSize = blockSize;
    jstring jStrBufferSize = (*env)->NewStringUTF(env, "io.file.buffer.size");
    jstring jStrReplication = (*env)->NewStringUTF(env, "dfs.replication");
    jstring jStrBlockSize = (*env)->NewStringUTF(env, "dfs.block.size");


    //bufferSize
    if (!bufferSize) {
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jConfiguration,
                         HADOOP_CONF, "getInt", "(Ljava/lang/String;I)I",
                         jStrBufferSize, 4096) != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                                       "Configuration::getInt");
            goto done;
        }
        jBufferSize = jVal.i;
    }

    if ((flags & O_WRONLY) && (flags & O_APPEND) == 0) {
        //replication

        if (!replication) {
            if (invokeMethod(env, &jVal, &jExc, INSTANCE, jConfiguration,
                             HADOOP_CONF, "getInt", "(Ljava/lang/String;I)I",
                             jStrReplication, 1) != 0) {
                errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                                           "Configuration::getInt");
                goto done;
            }
            jReplication = jVal.i;
        }

        //blockSize
        if (!blockSize) {
            if (invokeMethod(env, &jVal, &jExc, INSTANCE, jConfiguration,
                             HADOOP_CONF, "getLong", "(Ljava/lang/String;J)J",
                             jStrBlockSize, 67108864)) {
                errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                                           "FileSystem::%s(%s)", method,
                                           signature);
                goto done;
            }
            jBlockSize = jVal.j;
        }
    }

    /* Create and return either the FSDataInputStream or
       FSDataOutputStream references jobject jStream */

    // READ?
    if ((flags & O_WRONLY) == 0) {
        if( -1 == codec )
        {//java-> open or open2  open(path, bufferSize)
            if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                        method, signature, jPath, jBufferSize)) {
                errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                        "FileSystem::%s(%s)", method,
                        signature);
                goto done;
            }
        }
        else
        {//java->openWithCodec   openWithCodec(path, codec, bufferSize)
            if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                        method, signature, jPath, jCodec, jBufferSize)) {
                errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                        "FileSystem::%s(%s)", method,
                        signature);
                goto done;
            }
        }
    }
    // WRITE/APPEND?
    else if ((flags & O_WRONLY) && (flags & O_APPEND)) {
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                    method, signature, jPath)) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                    "FileSystem::%s(%s)", method,
                    signature);
            goto done;
        }
    }


    // WRITE/CREATE
    else {
        jboolean jOverWrite = 1;
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                         method, signature, jPath, jOverWrite,
                         jBufferSize, jReplication, jBlockSize)) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                                       "FileSystem::%s(%s)", method,
                                       signature);
            goto done;
        }
    }

    file = malloc(sizeof(struct hdfsFile_internal));
    if (!file) {
        errno = ENOMEM;
        return NULL;
    }
    file->file = (*env)->NewGlobalRef(env, jVal.l);
    file->type = (((flags & O_WRONLY) == 0) ? INPUT : OUTPUT);

    destroyLocalReference(env, jVal.l);

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jStrBufferSize);
    destroyLocalReference(env, jStrReplication);
    destroyLocalReference(env, jStrBlockSize);
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jPath);

    return file;
}

hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags,
                      int bufferSize, short replication, tSize blockSize)
{
    return _hdfsOpen(fs, path, flags, bufferSize, replication, blockSize, -1);
}

hdfsFile hdfsOpenFileWithDeCompress(hdfsFS fs, const char* path, int flags,
                      int bufferSize, short replication, tSize blockSize, CompressType compress)
{
    return _hdfsOpen(fs, path, flags, bufferSize, replication, blockSize, compress);
}
int hdfsCloseFile(hdfsFS fs, hdfsFile file)
{
    // JAVA EQUIVALENT:
    //  file.close

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
      errno = EINTERNAL;
      return -2;
    }

    //Parameters
    jobject jStream = (jobject)(file ? file->file : NULL);

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!file || file->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //The interface whose 'close' method to be called
    const char* interface = (file->type == INPUT) ?
        HADOOP_ISTRM : HADOOP_OSTRM;

    if (invokeMethod(env, NULL, &jExc, INSTANCE, jStream, interface,
                     "close", "()V") != 0) {
        errno = errnoFromException(jExc, env, "%s::close", interface);
        return -1;
    }

    //De-allocate memory
    free(file);
    (*env)->DeleteGlobalRef(env, jStream);

    return 0;
}

LineReader createLineReader(hdfsFile f){

    if (!f || f->type != INPUT) {
        errno = EBADF;
        return NULL;
    }


    LineReader lr = malloc(sizeof(struct LineReader_internal));
    if (lr == NULL) {
        errno = ENOMEM;
        return NULL;
    }

    lr->hdfsF = f;

    lr->buffer = (char*)malloc(64*1024 + 1);
    if (lr->buffer == NULL){
        errno = ENOMEM;
        free(lr);
        return NULL;
    }

    lr->bufferSize = 64*1024;
    lr->bufferPosn = 0;
    lr->bufferLength = 0;
    lr->eof = 0;
    lr->prevCharCR = 0;
    lr->pos = 0;
    return lr;
}

// >=0 - real pos
// < 0 - error
int seekLineReader(hdfsFS fs, LineReader lr, tOffset desiredPos){
    if ((fs == NULL) || (lr == NULL)){
      errno = EINVAL;
      return -2;
    }

    int ret = hdfsSeek(fs, lr->hdfsF, desiredPos);
    if (ret == 0){ // reset LineReader internal state
        lr->bufferPosn = 0;
        lr->bufferLength = 0;
        lr->eof = 0;
        lr->prevCharCR = 0;
        lr->pos = desiredPos;
    }

    return ret;
}

// >= 0 - ok
// <  0 - error
tOffset getLineReaderPos(hdfsFS fs, LineReader lr){
    if ((fs == NULL) || (lr == NULL)){
      errno = EINVAL;
      return -2;
    }

    return lr->pos;
}

// >= 0 - has data
// -1 - eof
// -2 - error
int readLineByLineReader(hdfsFS fs, LineReader lr, void** line){

    static int MAX_LINE_SIZE = 100 * 1024 * 1024;

    if ((fs == NULL) || (lr == NULL) || (line == NULL)){
      errno = EINVAL;
      return -2;
    }

    int lineStartPos = lr->bufferPosn;
    int lineLength = 0;
    int found = 0;
    int bufferEndPos = 0;
    int countNR = 0;

    while (!found){

        bufferEndPos = lr->bufferPosn + lr->bufferLength;
        while (lr->bufferPosn < bufferEndPos) {
            if (lr->buffer[lr->bufferPosn] == '\r'){
                countNR++;
                lr->prevCharCR = 1;
                lr->bufferPosn++;
                found = 1;
                break;

            } else if (lr->buffer[lr->bufferPosn] == '\n'){
                countNR++;
                // 如果前面为CR, 则吃掉这个LF，并且这一定是新行的开始
                if (lr->prevCharCR){
                    lineStartPos++;
                    lr->prevCharCR = 0;
                    lr->bufferPosn++;
                    continue;
                }

                // 如果前面不是CR，则为行的尾部
                lr->bufferPosn++;
                found = 1;
                break;

            } else if (lr->prevCharCR) {
                lr->prevCharCR = 0;
            }

            lr->bufferPosn++;
            lineLength++;
        }

        if (!found){

            // 如果这是文件的最后一行，就把这一行送出
            if (lr->eof){
                if (lineLength == 0){
                    return -1;
                } else {
                    found = 1;
                    continue;
                }
            }

            // 如果当前buffer中没有数据
            if (lineLength == 0) {
                lineStartPos = 0;
                lr->bufferPosn = 0;
                tSize readSize = hdfsRead(fs, lr->hdfsF, lr->buffer, lr->bufferSize);
                if (readSize == -1) {
                    // 出现错误
                    errno = EINTERNAL;
                    return -2;
                }
                if (readSize != lr->bufferSize){
                    // 文件读到尾部
                    lr->eof = 1;
                }
                lr->bufferLength = readSize;

            } else if (lineLength < lr->bufferSize) {
                // 如果长度小于_bufferSize, 则在缓冲区中移动到头部
                memmove(lr->buffer, lr->buffer + lineStartPos, lineLength);
                lineStartPos = 0;
                lr->bufferPosn = lineLength;
                tSize readSize = hdfsRead(fs, lr->hdfsF, lr->buffer + lineLength, lr->bufferSize - lineLength);
                if (readSize == -1) {
                    // 出现错误
                    errno = EINTERNAL;
                    return -2;
                }
                if (readSize != lr->bufferSize - lineLength){
                    // 文件读到尾部
                    lr->eof = 1;
                }
                lr->bufferLength = readSize;
            } else if (lineLength == lr->bufferSize) {
                // 如果当前buffer中存有全部数据，则说明buffer空间不够，则扩大一倍buffer空间, 超过最大行大小限制，直接报错
                if (lr->bufferSize >= MAX_LINE_SIZE){
                     errno = EINTERNAL;
                     return -2;
                }
                char* newBuffer = (char*)malloc(2*lr->bufferSize + 1);
                if (newBuffer == NULL) {
                  errno = ENOMEM;
                  return -2;
                }
                memmove(newBuffer, lr->buffer, lr->bufferSize);
                free(lr->buffer);
                lr->buffer = newBuffer;
                tSize readSize = hdfsRead(fs, lr->hdfsF, lr->buffer + lr->bufferSize, lr->bufferSize);
                if (readSize == -1) {
                    // 出现错误
                    errno = EINTERNAL;
                    return -2;
                }
                if (readSize != lr->bufferSize){
                    // 文件读到尾部
                    lr->eof = 1;
                }
                lr->bufferSize = lr->bufferSize*2;
                lr->bufferLength = readSize;
            } else {
                // 不可能走到这里
               errno = EINTERNAL;
               return -2;
            }
        }
    }

    *line =  lr->buffer + lineStartPos;
    *((char*)(*line) + lineLength) = '\0';
    lr->bufferLength = bufferEndPos - lr->bufferPosn;
    lr->pos = lr->pos + lineLength + countNR;
    return lineLength;
}

void closeLineReader(LineReader lr){

    if (lr == NULL) {
      return;
    }

    free(lr->buffer);
    lr->buffer = NULL;
    lr->bufferSize = 0;

    free(lr);
}

SeqFile readSequenceFile(hdfsFS fs, const char* path)
{

    if ((path == NULL) || (fs == NULL)){
       errno = EINVAL;
       return NULL;
    }

    /*
        public NewSequenceFileReader(Path filePath)
    */

    /* Get the JNIEnv* corresponding to current thread */
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    /* Return value */
    SeqFile file = NULL;

    /* Create an object of org.apache.hadoop.fs.Path */
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    jobject jFS = (jobject)fs;

    jthrowable jExc = NULL;
    jobject jSeqReader;
    if ((jSeqReader = constructNewObjectOfClass(
            env,
            &jExc,
            HADOOP_SEQUENCEFILE_READER2,
            JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_FS), JAVA_VOID),
            jPath,
            jFS)) == NULL) {
      fprintf(stderr,"failed to construct NewSequenceFileReader\n");
      errno = errnoFromException(jExc, env, HADOOP_SEQUENCEFILE_READER2,
                                 "init");
      destroyLocalReference(env, jPath);
      return NULL;
    }

    file = malloc(sizeof(struct SeqFile_internal));
    if (!file) {
        errno = ENOMEM;
        return NULL;
    }
    file->file = (*env)->NewGlobalRef(env, jSeqReader);
    file->type = INPUT;
    file->readBuf = NULL;
    file->nextReadBufPos = 0;
    file->totalReadBufPos = 0;

    destroyLocalReference(env, jSeqReader);
    destroyLocalReference(env, jPath);

    return file;
}

// 0:ok; -1:error
static int flushSeqFile(SeqFile file, JNIEnv* env)
{
    //Parameters
    jobject jSeqFile = (jobject)(file->file);
    if (file->writeBufPos != 0) {
        jbyteArray dataArray = (*env)->NewByteArray(env, file->writeBufPos);
        if (dataArray == NULL) {
          errno = ENOMEM;
          return -1;
        }
        (*env)->SetByteArrayRegion(env, dataArray, 0, file->writeBufPos, file->writeBuf);
        jthrowable jExc = NULL;
        if (invokeMethod(env, NULL, &jExc, INSTANCE, jSeqFile, HADOOP_SEQUENCEFILE_WRITER2, "write", JMETHOD3("[B", "I", "I", "V"), dataArray, 0, file->writeBufPos) != 0) {
           errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileWriter::write");
           destroyLocalReference(env, dataArray);
           return -1;
        }
        destroyLocalReference(env, dataArray);
        file->writeBufPos = 0;
    }

    return 0;
}

// >= 0:correct; -1:error
tOffset getSeqFilePos(SeqFile file)
{
    // Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    // Sanity check
    if (!file) {
        errno = EBADF;
        return -1;
    }

    jobject jSeqFile = (jobject)(file->file);
    jvalue jVal;
    jthrowable jExc = NULL;

    if (file->type == OUTPUT) {

        // flush the write buffer
        if (flushSeqFile(file, env) != 0) {
            errno = EINTERNAL;
            return -1;
        }
        // Invoke org.apache.hadoop.mapred.NewSequenceFileWriter::getCurPos()
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jSeqFile, HADOOP_SEQUENCEFILE_WRITER2, "getCurPos", "()J") != 0) {
           errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileWriter::getCurPos");
           return -1;
        }
        return jVal.j;
    }

    if (file->type == INPUT) {

        // Invoke org.apache.hadoop.mapred.NewSequenceFileReader::getCurPos()
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jSeqFile, HADOOP_SEQUENCEFILE_READER2, "getCurPos", "()J") != 0) {
           errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileReader::getCurPos");
           return -1;
        }
        return jVal.j;
    }

    errno = EBADF;
    return -1;
}


SeqFile writeSequenceFile(hdfsFS fs, const char* path, const char* compressionTypeString, const char* codecString)
{
    /*
        public NewSequenceFileWriter(Path filePath, String compressionTypeString, String codecString) throws IOException
    */

    /* Get the JNIEnv* corresponding to current thread */
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }


    /* Return value */
    SeqFile file = NULL;

    /* Create an object of org.apache.hadoop.fs.Path */
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    jstring jStrCompressionType = (*env)->NewStringUTF(env, compressionTypeString);
    jstring jStrCodecString = (*env)->NewStringUTF(env, codecString);
    jthrowable jExc = NULL;
    jobject jSeqWriter;
    jobject jFS = (jobject)fs;
    if ((jSeqWriter = constructNewObjectOfClass(env, &jExc, HADOOP_SEQUENCEFILE_WRITER2, JMETHOD4(JPARAM(HADOOP_PATH), JPARAM(JAVA_STRING), JPARAM(JAVA_STRING), JPARAM(HADOOP_FS), JAVA_VOID), jPath, jStrCompressionType, jStrCodecString, jFS)) == NULL) {
      fprintf(stderr,"failed to construct NewSequenceFileWriter\n");
      errno = errnoFromException(jExc, env, HADOOP_SEQUENCEFILE_WRITER2, "init");
      destroyLocalReference(env, jPath);
      destroyLocalReference(env, jStrCodecString);
      destroyLocalReference(env, jStrCompressionType);
      return NULL;
    }

    file = malloc(sizeof(struct SeqFile_internal));
    if (!file) {
        errno = ENOMEM;
        return NULL;
    }
    file->file = (*env)->NewGlobalRef(env, jSeqWriter);
    file->type = OUTPUT;
    destroyLocalReference(env, jSeqWriter);
    destroyLocalReference(env, jStrCodecString);
    destroyLocalReference(env, jStrCompressionType);

    file->writeBuf = malloc(sizeof(char) * 64 * 1024);
	if (file->writeBuf == NULL) {
		errno = ENOMEM;
		return NULL;
	}
	file->writeBufSize = 64 * 1024;
	file->writeBufPos = 0;

    return file;
}

SeqFile appendSequenceFile(hdfsFS fs, const char* path) {
    /*
         public NewSequenceFileWriter(Path filePath, FileSystem fs) throws IOException
     */
    /* Get the JNIEnv* corresponding to current thread */
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
        errno = EINTERNAL;
        return NULL;
    }


    /* Return value */
    SeqFile file = NULL;

    /* Create an object of org.apache.hadoop.fs.Path */
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    jthrowable jExc = NULL;
    jobject jSeqWriter;
    jobject jFS = (jobject)fs;
    if ((jSeqWriter = constructNewObjectOfClass(
            env,
            &jExc,
            HADOOP_SEQUENCEFILE_WRITER2,
            JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_FS), JAVA_VOID),
            jPath,
            jFS)) == NULL) {
        fprintf(stderr,"failed to construct NewSequenceFileWriter for append\n");
        errno = errnoFromException(jExc, env, HADOOP_SEQUENCEFILE_WRITER2, "init");
        destroyLocalReference(env, jPath);
        return NULL;
    }

    file = malloc(sizeof(struct SeqFile_internal));
    if (!file) {
        errno = ENOMEM;
        return NULL;
    }

    file->file = (*env)->NewGlobalRef(env, jSeqWriter);
    file->type = OUTPUT;
    destroyLocalReference(env, jSeqWriter);

    file->writeBuf = malloc(sizeof(char) * 64 * 1024);
    if (file->writeBuf == NULL) {
        errno = ENOMEM;
        return NULL;
    }

    file->writeBufSize = 64 * 1024;
    file->writeBufPos = 0;

    return file;
}

int closeSequenceFile(hdfsFS fs, SeqFile file)
{


    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
      errno = EINTERNAL;
      return -2;
    }

    //Parameters
    jobject jSeqFile = (jobject)(file ? file->file : NULL);

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!file || file->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    if (file->type == OUTPUT){

        if (file->writeBufPos != 0) {
            jbyteArray dataArray = (*env)->NewByteArray(env, file->writeBufPos);
            if (dataArray == NULL) {
               errno = ENOMEM;
               return -1;
            }
            (*env)->SetByteArrayRegion(env, dataArray, 0, file->writeBufPos, file->writeBuf);
            jthrowable jExc = NULL;
            if (invokeMethod(env, NULL, &jExc, INSTANCE, jSeqFile, HADOOP_SEQUENCEFILE_WRITER2, "write",
                JMETHOD3("[B", "I", "I", "V"), dataArray, 0, file->writeBufPos) != 0) {
               errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileWriter::write");
               destroyLocalReference(env, dataArray);
               return -1;
            }
            destroyLocalReference(env, dataArray);
            file->writeBufPos = 0;
        }

        if (file->writeBuf != NULL) {
            free(file->writeBuf);
            file->writeBuf = NULL;
            file->writeBufSize = 0;
        }

    }

    //The interface whose 'close' method to be called
    const char* interface = (file->type == INPUT) ?
           HADOOP_SEQUENCEFILE_READER2 : HADOOP_SEQUENCEFILE_WRITER2;

    if (invokeMethod(env, NULL, &jExc, INSTANCE, jSeqFile, interface, "close", "()V") != 0) {
        errno = errnoFromException(jExc, env, "%s::close", interface);
        return -1;
    }

    if (file->type == INPUT){
        file->readBuf = NULL;
        file->nextReadBufPos = 0;
        file->totalReadBufPos = 0;
    }

    //De-allocate memory
    free(file);
    (*env)->DeleteGlobalRef(env, jSeqFile);

    return 0;

}

// 0:ok; -1:error
int seekSeqFile(SeqFile f, tOffset offset) {

    if (offset < 0) {
        errno = EINVAL;
        return -1;
    }

    // Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    // Error checking... make sure that this file is 'readable'
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    // init the related variable
    f->readBuf = NULL;
    f->nextReadBufPos = 0;
    f->totalReadBufPos = 0;

    // invoke org.apache.hadoop.mapred.NewSequenceFileReader::seek(long offset)
    jobject jSeqReader = (jobject)f->file;
    jthrowable jExc = NULL;
    jlong newOffset = offset;
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jSeqReader, HADOOP_SEQUENCEFILE_READER2, "seek", "(J)V", newOffset) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileReader::seek()");
        return -1;
    }

    return 0;
}

// >=0:offset after sync ; -1:error
tOffset syncSeqFile(SeqFile f, tOffset offset) {

    if (offset < 0) {
        errno = EINVAL;
        return -1;
    }

    // Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    // Error checking... make sure that this file is 'readable'
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    // init the related variable
    f->readBuf = NULL;
    f->nextReadBufPos = 0;
    f->totalReadBufPos = 0;

    // invoke org.apache.hadoop.mapred.NewSequenceFileReader::seek(long offset)
    jobject jSeqReader = (jobject)f->file;
    jvalue jVal;
    jthrowable jExc = NULL;
    jlong newOffset = offset;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jSeqReader, HADOOP_SEQUENCEFILE_READER2, "sync", "(J)J", newOffset) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileReader::sync()");
        return -1;
    } else {
       return jVal.j;
    }
}

// 0 - ok
// 1 - finish
// -1 - error
int readNextRecordFromSeqFile(hdfsFS fs, SeqFile f, void** key, int* keyLen, void** value, int* valueLen) {

    // check NULL pointer
    if (key == NULL || keyLen == NULL || value == NULL || valueLen == NULL) {
        errno = EINVAL;
        return -1;
    }

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }
    //Error checking... make sure that this file is 'readable'
    if (f->type != INPUT) {
        fprintf(stderr, "Cannot read from a non-SequeceFileReader!\n");
        errno = EINVAL;
        return -1;
    }

    // 如果read buffer中还有记录，则从buf中读入
    if ((f->readBuf != NULL) && (f->nextReadBufPos < f->totalReadBufPos)) {
    	int* theLen = (int*)((char*)f->readBuf + f->nextReadBufPos);
    	*keyLen = *theLen;
    	f->nextReadBufPos += sizeof(int);
    	*key = (char*)f->readBuf + f->nextReadBufPos;
    	f->nextReadBufPos += *keyLen;
    	theLen = (int*)((char*)f->readBuf + f->nextReadBufPos);
    	*valueLen = *theLen;
    	f->nextReadBufPos += sizeof(int);
    	*value = (char*)f->readBuf + f->nextReadBufPos;
    	f->nextReadBufPos += *valueLen;
    	return 0;
    }

    // 否则调用java reader类重新读入一个新的buf
    jobject jSeqReader = (jobject)f->file;

    jthrowable jExc = NULL;
    jvalue jVal;

    jint totalSize = 0;
        // invoke   public int readRecords() throws IOException
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jSeqReader, HADOOP_SEQUENCEFILE_READER2, "readRecords", "()I") != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileReader::readRecords()");
            return -1;
        }
        totalSize = jVal.i;
        if (totalSize == 0) return 1;

    // public ByteBuffer getDirectBuf()
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jSeqReader, HADOOP_SEQUENCEFILE_READER2, "getDirectBuf", "()Ljava/nio/ByteBuffer;") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileReader::getDirectBuf()");
        return -1;
    }

    jobject jbyteBuf = jVal.l;
    f->readBuf = (*env)->GetDirectBufferAddress(env, jbyteBuf);
    f->nextReadBufPos = 0;
    f->totalReadBufPos = totalSize;
    destroyLocalReference(env, jbyteBuf);

    // read buffer中应该有记录了，则从buf中读入
    if ((f->readBuf != NULL) && (f->nextReadBufPos < f->totalReadBufPos)) {
    	int* theLen = (int*)((char*)f->readBuf + f->nextReadBufPos);
    	*keyLen = *theLen;
    	f->nextReadBufPos += sizeof(int);
    	*key = (char*)f->readBuf + f->nextReadBufPos;
    	f->nextReadBufPos += *keyLen;
    	theLen = (int*)((char*)f->readBuf + f->nextReadBufPos);
    	*valueLen = *theLen;
    	f->nextReadBufPos += sizeof(int);
    	*value = (char*)f->readBuf + f->nextReadBufPos;
    	f->nextReadBufPos += *valueLen;
    	return 0;
    }

    // 不应该会走到这
    return -1;
}

// 0 - ok
// 1 - finish
// -1 - error
int readFirstRecordFromSeqFile(hdfsFS fs, SeqFile f, void** key, int* keyLen, void** value, int* valueLen) {

    // check NULL pointer
    if (key == NULL || keyLen == NULL || value == NULL || valueLen == NULL) {
        errno = EINVAL;
        return -1;
    }

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }
    //Error checking... make sure that this file is 'readable'
    if (f->type != INPUT) {
        fprintf(stderr, "Cannot read from a non-SequeceFileReader!\n");
        errno = EINVAL;
        return -1;
    }

    jobject jSeqReader = (jobject)f->file;

    jthrowable jExc = NULL;
    jvalue jVal;

    jint totalSize = 0;
        // invoke   public int readRecords() throws IOException
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jSeqReader, HADOOP_SEQUENCEFILE_READER2, "readFirstRecord", "()I") != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileReader::readFirstRecord()");
            return -1;
        }
        totalSize = jVal.i;
        if (totalSize == 0) return 1;

    // public ByteBuffer getDirectBuf()
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jSeqReader, HADOOP_SEQUENCEFILE_READER2, "getDirectBuf", "()Ljava/nio/ByteBuffer;") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileReader::getDirectBuf()");
        return -1;
    }

    jobject jbyteBuf = jVal.l;
    f->readBuf = (*env)->GetDirectBufferAddress(env, jbyteBuf);
    f->nextReadBufPos = 0;
    f->totalReadBufPos = totalSize;
    destroyLocalReference(env, jbyteBuf);

    if ((f->readBuf != NULL) && (f->nextReadBufPos < f->totalReadBufPos)) {
    	int* theLen = (int*)((char*)f->readBuf + f->nextReadBufPos);
    	*keyLen = *theLen;
    	f->nextReadBufPos += sizeof(int);
    	*key = (char*)f->readBuf + f->nextReadBufPos;
    	f->nextReadBufPos += *keyLen;
    	theLen = (int*)((char*)f->readBuf + f->nextReadBufPos);
    	*valueLen = *theLen;
    	f->nextReadBufPos += sizeof(int);
    	*value = (char*)f->readBuf + f->nextReadBufPos;
    	f->nextReadBufPos += *valueLen;
    	return 0;
    }

    return -1;
}

// 0 - ok
// 1 - finish
// -1 - error
int readLastRecordFromSeqFile(hdfsFS fs, SeqFile f, void** key, int* keyLen, void** value, int* valueLen) {

    // check NULL pointer
    if (key == NULL || keyLen == NULL || value == NULL || valueLen == NULL) {
        errno = EINVAL;
        return -1;
    }

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }
    //Error checking... make sure that this file is 'readable'
    if (f->type != INPUT) {
        fprintf(stderr, "Cannot read from a non-SequeceFileReader!\n");
        errno = EINVAL;
        return -1;
    }

    jobject jSeqReader = (jobject)f->file;

    jthrowable jExc = NULL;
    jvalue jVal;

    jint totalSize = 0;
        // invoke   public int readRecords() throws IOException
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jSeqReader, HADOOP_SEQUENCEFILE_READER2, "readLastRecord", "()I") != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileReader::readLastRecord()");
            return -1;
        }
        totalSize = jVal.i;
        if (totalSize == 0) return 1;

    // public ByteBuffer getDirectBuf()
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jSeqReader, HADOOP_SEQUENCEFILE_READER2, "getDirectBuf", "()Ljava/nio/ByteBuffer;") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileReader::getDirectBuf()");
        return -1;
    }

    jobject jbyteBuf = jVal.l;
    f->readBuf = (*env)->GetDirectBufferAddress(env, jbyteBuf);
    f->nextReadBufPos = 0;
    f->totalReadBufPos = totalSize;
    destroyLocalReference(env, jbyteBuf);

    if ((f->readBuf != NULL) && (f->nextReadBufPos < f->totalReadBufPos)) {
    	int* theLen = (int*)((char*)f->readBuf + f->nextReadBufPos);
    	*keyLen = *theLen;
    	f->nextReadBufPos += sizeof(int);
    	*key = (char*)f->readBuf + f->nextReadBufPos;
    	f->nextReadBufPos += *keyLen;
    	theLen = (int*)((char*)f->readBuf + f->nextReadBufPos);
    	*valueLen = *theLen;
    	f->nextReadBufPos += sizeof(int);
    	*value = (char*)f->readBuf + f->nextReadBufPos;
    	f->nextReadBufPos += *valueLen;
    	return 0;
    }

    return -1;
}


// 0 - ok
// -1 - error
int writeRecordIntoSeqFile(hdfsFS fs, SeqFile f, const void* key, int keyLen, const void* value, int valueLen){


    if (4 + 4 + keyLen + valueLen < 0) {
        errno = EINVAL;
        return -1;
    }

    int ret = 0;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }
    //Error checking... make sure that this file is 'writable'
    if (f->type != OUTPUT) {
        fprintf(stderr, "Cannot write into a non-SequeceFileWriter!\n");
        errno = EINVAL;
        return -1;
    }

    jobject jSeqFile = (jobject)f->file;

    if ((keyLen < 0) || (valueLen < 0)){
        errno = EINVAL;
        return -1;
    }

    if ((keyLen != 0) && (key == NULL)) {
       errno = EINVAL;
       return -1;
    }

    if ((valueLen != 0) && (value == NULL)) {
       errno = EINVAL;
       return -1;
    }

    int remaining = f->writeBufSize - f->writeBufPos;

    if ((remaining < (4 + 4 + keyLen + valueLen)) && (f->writeBufPos != 0)) {
        jbyteArray dataArray = (*env)->NewByteArray(env, f->writeBufPos);
        if (dataArray == NULL) {
          errno = ENOMEM;
          return -1;
        }
        (*env)->SetByteArrayRegion(env, dataArray, 0, f->writeBufPos, f->writeBuf);
        jthrowable jExc = NULL;
        if (invokeMethod(env, NULL, &jExc, INSTANCE, jSeqFile, HADOOP_SEQUENCEFILE_WRITER2, "write", JMETHOD3("[B", "I", "I", "V"), dataArray, 0, f->writeBufPos) != 0) {
           errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileWriter::write");
           destroyLocalReference(env, dataArray);
           return -1;
        }
        destroyLocalReference(env, dataArray);
        remaining = f->writeBufSize;
        f->writeBufPos = 0;
    }

    if (remaining >= (4 + 4 + keyLen + valueLen)) {
    	int* theLen = (int*)((char*)f->writeBuf + f->writeBufPos);
    	*theLen = keyLen + valueLen;
    	endian_swap((unsigned int*)theLen);
    	f->writeBufPos += 4;
    	theLen = (int*)((char*)f->writeBuf + f->writeBufPos);
    	*theLen = keyLen;
    	endian_swap((unsigned int*)theLen);
    	f->writeBufPos += 4;
    	memcpy(f->writeBuf + f->writeBufPos, key, keyLen);
    	f->writeBufPos += keyLen;
    	memcpy(f->writeBuf + f->writeBufPos, value, valueLen);
    	f->writeBufPos += valueLen;
    	return 0;
    }

    // key/value较大，直接写下去
    jbyteArray jkeyArray = (*env)->NewByteArray(env, keyLen);
    if (jkeyArray == NULL) {
       errno = ENOMEM;
       return -1;
    }
    jbyteArray jvalueArray = (*env)->NewByteArray(env, valueLen);
    if (jvalueArray == NULL) {
       errno = ENOMEM;
       if (jkeyArray != NULL)
         destroyLocalReference(env, jkeyArray);
       return -1;
    }
    (*env)->SetByteArrayRegion(env, jkeyArray, 0, keyLen, key);
    (*env)->SetByteArrayRegion(env, jvalueArray, 0, valueLen, value);


    //Caught exception
    jthrowable jExc = NULL;
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jSeqFile, HADOOP_SEQUENCEFILE_WRITER2, "writeKeyVal", JMETHOD2("[B", "[B", "V"), jkeyArray, jvalueArray) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileWriter::writeKeyVal");
        ret = -1;
    }

    destroyLocalReference(env, jkeyArray);
    destroyLocalReference(env, jvalueArray);

    return ret;
}


// -1 error
// 0 ok
int syncSeqFileFs(hdfsFS fs, SeqFile f) {
    // JAVA EQUIVALENT
    //  write.sync();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //Error checking... make sure that this file is 'writable'
    if ( !f || f->type != OUTPUT) {
        fprintf(stderr, "Cannot write into a non-SequeceFileWriter!\n");
        errno = EINVAL;
        return -1;
    }

    jobject jSeqFile = (jobject)f->file;
    //Caught exception
    jthrowable jExc = NULL;
    if (0 != flushSeqFile(f, env)) {
        return -1;
    }

    if (invokeMethod(env, NULL, &jExc, INSTANCE, jSeqFile, HADOOP_SEQUENCEFILE_WRITER2, "sync", "()V") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred.NewSequenceFileWriter:::sync");
        return -1;
    }

    return 0;
}

int hdfsExists(hdfsFS fs, const char *path)
{
    JNIEnv *env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -2;
    }

    jobject jPath = constructNewObjectOfPath(env, path);
    jvalue  jVal;
    jthrowable jExc = NULL;
    jobject jFS = (jobject)fs;

    if (jPath == NULL) {
        return -1;
    }

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "exists", JMETHOD1(JPARAM(HADOOP_PATH), "Z"),
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::exists");
        return -1;
    }

    return jVal.z ? 0 : -1;
}

int hdfsRecoverFile(hdfsFS fs, const char *path)
{
    JNIEnv *env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -2;
    }

    jobject jPath = constructNewObjectOfPath(env, path);
    jthrowable jExc = NULL;
    jobject jFS = (jobject)fs;

    if (jPath == NULL) {
        errno = ENOMEM;
        return -1;
    }

    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "recoverFile", JMETHOD1(JPARAM(HADOOP_PATH), "V"),
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::recoverFile");
        destroyLocalReference(env, jPath);
        return -1;
    }

    destroyLocalReference(env, jPath);
    return 0;
}



tSize hdfsRead(hdfsFS fs, hdfsFile f, void* buffer, tSize length)
{
    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(bR);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : NULL);

    jbyteArray jbRarray;
    jint noReadBytes = 0;
    jvalue jVal;
    jthrowable jExc = NULL;

    int hasReadBytes = 0;

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //Error checking... make sure that this file is 'readable'
    if (f->type != INPUT) {
        fprintf(stderr, "Cannot read from a non-InputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    int exception = 0;

    if (length == 0)
      return 0;

    tSize splitSize = 0;
    if (length <= 1024*1024) {
      splitSize = length;
    } else {
      splitSize = 1024*1024;
    }

    //Read the requisite bytes
    jbRarray = (*env)->NewByteArray(env, splitSize);
    if (jbRarray == NULL) {
       errno = ENOMEM;
       return -1;
    }

    while (hasReadBytes < length) {
        if ( length - hasReadBytes < 1024*1024 )
           splitSize = length - hasReadBytes;
        else
           splitSize = 1024*1024;

        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jInputStream, HADOOP_ISTRM,
                         "read", JMETHOD3("[B", "I", "I", "I") , jbRarray, 0, splitSize) != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs.FSDataInputStream::read");
            exception = 1;
            break;
        } else {
            noReadBytes = jVal.i;
            if (noReadBytes >= 0) {
                (*env)->GetByteArrayRegion(env, jbRarray, 0, noReadBytes, buffer+hasReadBytes);
                hasReadBytes += noReadBytes;
            }  else {
                //This is a valid case: there aren't any bytes left to read!
                break;
            }
            errno = 0;
        }


    }

    if (jbRarray != NULL) {
      destroyLocalReference(env, jbRarray);
      jbRarray = NULL;
    }

    if (exception == 1) return -1;
    return hasReadBytes;
}



tSize hdfsPread(hdfsFS fs, hdfsFile f, tOffset position,
                void* buffer, tSize length)
{
    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(pos, bR, 0, length);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : NULL);

    jbyteArray jbRarray;
    jint noReadBytes = 0;
    jvalue jVal;
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //Error checking... make sure that this file is 'readable'
    if (f->type != INPUT) {
        fprintf(stderr, "Cannot read from a non-InputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    //Read the requisite bytes
    jbRarray = (*env)->NewByteArray(env, length);
    if (jbRarray == NULL) {
       errno = ENOMEM;
       return -1;
    }
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jInputStream, HADOOP_ISTRM,
                     "read", "(J[BII)I", position, jbRarray, 0, length) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FSDataInputStream::read");
        noReadBytes = -1;
    }
    else {
        noReadBytes = jVal.i;
        if (noReadBytes > 0) {
            (*env)->GetByteArrayRegion(env, jbRarray, 0, noReadBytes, buffer);
        }  else {
            //This is a valid case: there aren't any bytes left to read!
            noReadBytes = 0;
        }
        errno = 0;
    }
    destroyLocalReference(env, jbRarray);

    return noReadBytes;
}



tSize hdfsWrite(hdfsFS fs, hdfsFile f, const void* buffer, tSize length)
{
    // JAVA EQUIVALENT
    // byte b[] = str.getBytes();
    // fso.write(b);

    if (length < 0) {
      errno = EINVAL;
      return -1;
    }

    if ((length != 0) && (buffer == NULL)){
      errno = EINVAL;
      return -1;
    }

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jOutputStream = (jobject)(f ? f->file : 0);
    jbyteArray jbWarray;

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    if (length < 0) {
    	errno = EINVAL;
    	return -1;
    }

    //Error checking... make sure that this file is 'writable'
    if (f->type != OUTPUT) {
        fprintf(stderr, "Cannot write into a non-OutputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    tSize hasWriten = 0;
    tSize curSize = 0;
    jbWarray = NULL;
    for (curSize = length - hasWriten; curSize > 0; curSize = length - hasWriten) {

        if (curSize > 1024*1024)
            curSize = 1024*1024;

        if (jbWarray == NULL) {
            jbWarray = (*env)->NewByteArray(env, curSize);
            if (jbWarray == NULL) {
               errno = ENOMEM;
               return -1;
            }
        }

        (*env)->SetByteArrayRegion(env, jbWarray, 0, curSize, buffer + hasWriten);
        if (invokeMethod(env, NULL, &jExc, INSTANCE, jOutputStream, HADOOP_OSTRM, "write",
                         "([BII)V", jbWarray, 0, curSize) != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs.FSDataOutputStream::write");
            length = -1;
            break;
        }

        hasWriten += curSize;

    }

    if (jbWarray != NULL) {
        destroyLocalReference(env, jbWarray);
        jbWarray = NULL;
    }

    return length;
 }


int hdfsSeek(hdfsFS fs, hdfsFile f, tOffset desiredPos)
{
    // JAVA EQUIVALENT
    //  fis.seek(pos);

    if (desiredPos < 0){
       errno = EINVAL;
       return -1;
    }

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : 0);

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, &jExc, INSTANCE, jInputStream, HADOOP_ISTRM,
                     "seek", "(J)V", desiredPos) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FSDataInputStream::seek");
        return -1;
    }

    return 0;
}



tOffset hdfsTell(hdfsFS fs, hdfsFile f)
{
    // JAVA EQUIVALENT
    //  pos = f.getPos();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jStream = (jobject)(f ? f->file : 0);

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    const char* interface = (f->type == INPUT) ?
        HADOOP_ISTRM : HADOOP_OSTRM;

    jlong currentPos  = -1;
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStream,
                     interface, "getPos", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FSDataInputStream::getPos");
        return -1;
    }
    currentPos = jVal.j;

    return (tOffset)currentPos;
}



int hdfsFlush(hdfsFS fs, hdfsFile f)
{
    // JAVA EQUIVALENT
    //  fos.flush();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jOutputStream = (jobject)(f ? f->file : 0);

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type != OUTPUT) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, &jExc, INSTANCE, jOutputStream,
                     HADOOP_OSTRM, "sync", "()V") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FSDataOutputStream::sync");
        return -1;
    }

    return 0;
}

int hdfsSync(hdfsFS fs, hdfsFile file)
{
    return hdfsFlush(fs, file);
}

int hdfsTruncate(hdfsFS fs, const char *path, tOffset length)
{
    if (length < 0) {
        errno = EINVAL;
        return -1;
    }

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jPath = constructNewObjectOfPath(env, path);
    jthrowable jExc = NULL;
    jobject jFS = (jobject)fs;

    if (NULL == jPath) {
        errno = ENOMEM;
        return -1;
    }

    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "truncate", JMETHOD2(JPARAM(HADOOP_PATH), "J", "V"), jPath, length) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::truncate");
        destroyLocalReference(env, jPath);
        return -1;
    }

    destroyLocalReference(env, jPath);
    return 0;

}


int hdfsAvailable(hdfsFS fs, hdfsFile f)
{
    // JAVA EQUIVALENT
    //  fis.available();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : 0);

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    jint available = -1;
    jvalue jVal;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jInputStream,
                     HADOOP_ISTRM, "available", "()I") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FSDataInputStream::available");
        return -1;
    }
    available = jVal.i;

    return available;
}



int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    //JAVA EQUIVALENT
    //  FileUtil::copy(srcFS, srcPath, dstFS, dstPath,
    //                 deleteSource = false, conf)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jSrcFS = (jobject)srcFS;
    jobject jDstFS = (jobject)dstFS;
    jobject jSrcPath = NULL;
    jobject jDstPath = NULL;

    jSrcPath = constructNewObjectOfPath(env, src);
    if (jSrcPath == NULL) {
        return -1;
    }

    jDstPath = constructNewObjectOfPath(env, dst);
    if (jDstPath == NULL) {
        destroyLocalReference(env, jSrcPath);
        return -1;
    }

    int retval = 0;

    //Create the org.apache.hadoop.conf.Configuration object
    jobject jConfiguration =
        constructNewObjectOfClass(env, NULL, HADOOP_CONF, "()V");
    if (jConfiguration == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        destroyLocalReference(env, jSrcPath);
        destroyLocalReference(env, jDstPath);
        return -1;
    }

    //FileUtil::copy
    jboolean deleteSource = 0; //Only copy
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, STATIC,
                     NULL, "org/apache/hadoop/fs/FileUtil", "copy",
                     "(Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;ZLorg/apache/hadoop/conf/Configuration;)Z",
                     jSrcFS, jSrcPath, jDstFS, jDstPath, deleteSource,
                     jConfiguration) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileUtil::copy");
        retval = -1;
        goto done;
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jSrcPath);
    destroyLocalReference(env, jDstPath);

    return retval;
}



int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    //JAVA EQUIVALENT
    //  FileUtil::copy(srcFS, srcPath, dstFS, dstPath,
    //                 deleteSource = true, conf)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }


    //Parameters
    jobject jSrcFS = (jobject)srcFS;
    jobject jDstFS = (jobject)dstFS;

    jobject jSrcPath = NULL;
    jobject jDstPath = NULL;

    jSrcPath = constructNewObjectOfPath(env, src);
    if (jSrcPath == NULL) {
        return -1;
    }

    jDstPath = constructNewObjectOfPath(env, dst);
    if (jDstPath == NULL) {
        destroyLocalReference(env, jSrcPath);
        return -1;
    }

    int retval = 0;

    //Create the org.apache.hadoop.conf.Configuration object
    jobject jConfiguration =
        constructNewObjectOfClass(env, NULL, HADOOP_CONF, "()V");
    if (jConfiguration == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        destroyLocalReference(env, jSrcPath);
        destroyLocalReference(env, jDstPath);
        return -1;
    }

    //FileUtil::copy
    jboolean deleteSource = 1; //Delete src after copy
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, STATIC, NULL,
                     "org/apache/hadoop/fs/FileUtil", "copy",
                "(Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;ZLorg/apache/hadoop/conf/Configuration;)Z",
                     jSrcFS, jSrcPath, jDstFS, jDstPath, deleteSource,
                     jConfiguration) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileUtil::copy(move)");
        retval = -1;
        goto done;
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jSrcPath);
    destroyLocalReference(env, jDstPath);

    return retval;
}



int hdfsDelete(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  File f = new File(path);
    //  bool retval = fs.delete(f);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //Create an object of java.io.File
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Delete the file
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "delete", "(Lorg/apache/hadoop/fs/Path;)Z",
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::delete");
        return -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return (jVal.z) ? 0 : -1;
}



int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath)
{
    // JAVA EQUIVALENT:
    //  Path old = new Path(oldPath);
    //  Path new = new Path(newPath);
    //  fs.rename(old, new);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //Create objects of org.apache.hadoop.fs.Path
    jobject jOldPath = NULL;
    jobject jNewPath = NULL;

    jOldPath = constructNewObjectOfPath(env, oldPath);
    if (jOldPath == NULL) {
        return -1;
    }

    jNewPath = constructNewObjectOfPath(env, newPath);
    if (jNewPath == NULL) {
        destroyLocalReference(env, jOldPath);
        return -1;
    }

    //Rename the file
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS, "rename",
                     JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_PATH), "Z"),
                     jOldPath, jNewPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::rename");
        return -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jOldPath);
    destroyLocalReference(env, jNewPath);

    return (jVal.z) ? 0 : -1;
}

char* hdfsGetFileCheckSumMD5(hdfsFS fs, const char* path) {
    // JAVA EQUIVALENT:
    //  Path p = fs.getWorkingDirectory();
    //  return p.toString()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
        errno = EINTERNAL;
        return NULL;
    }

    jobject jFS = (jobject)fs;
    jobject jPath = constructNewObjectOfPath(env, path);
    if (NULL == jPath) {
        errno = ENOMEM;
        return NULL;
    }

    jvalue jVal;
    jthrowable jExc = NULL;

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS, "getFileChecksumValue",
            JMETHOD1(JPARAM(HADOOP_PATH), JPARAM(JAVA_STRING)), jPath) != 0 || jVal.l == NULL) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                "FileSystem::GetFileChecksumValue");
        destroyLocalReference(env, jPath);
        return NULL;
    }

    jstring jChecksumString = jVal.l;

    const char *jChecksumChars = (const char*)
        		         ((*env)->GetStringUTFChars(env, jChecksumString, NULL));

    int len = strlen(jChecksumChars);
    char* buffer = (char*)malloc(len + 1);
    memset(buffer, 0, len + 1);
    //Copy to user-provided buffer
    strncpy(buffer, jChecksumChars, len);

    //Delete unnecessary local references
    (*env)->ReleaseStringUTFChars(env, jChecksumString, jChecksumChars);

    destroyLocalReference(env, jChecksumString);
    destroyLocalReference(env, jPath);

    return buffer;
}


char* hdfsGetWorkingDirectory(hdfsFS fs, char* buffer, size_t bufferSize)
{
    // JAVA EQUIVALENT:
    //  Path p = fs.getWorkingDirectory();
    //  return p.toString()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;
    jobject jPath = NULL;
    jvalue jVal;
    jthrowable jExc = NULL;

    //FileSystem::getWorkingDirectory()
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS,
                     HADOOP_FS, "getWorkingDirectory",
                     "()Lorg/apache/hadoop/fs/Path;") != 0 ||
        jVal.l == NULL) {
        errno = errnoFromException(jExc, env, "FileSystem::"
                                   "getWorkingDirectory");
        return NULL;
    }
    jPath = jVal.l;

    //Path::toString()
    jstring jPathString;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jPath,
                     "org/apache/hadoop/fs/Path", "toString",
                     "()Ljava/lang/String;") != 0) {
        errno = errnoFromException(jExc, env, "Path::toString");
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jPathString = jVal.l;

    const char *jPathChars = (const char*)
        ((*env)->GetStringUTFChars(env, jPathString, NULL));

    //Copy to user-provided buffer
    strncpy(buffer, jPathChars, bufferSize);

    //Delete unnecessary local references
    (*env)->ReleaseStringUTFChars(env, jPathString, jPathChars);

    destroyLocalReference(env, jPathString);
    destroyLocalReference(env, jPath);

    return buffer;
}



int hdfsSetWorkingDirectory(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  fs.setWorkingDirectory(Path(path));

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;
    int retval = 0;
    jthrowable jExc = NULL;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //FileSystem::setWorkingDirectory()
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "setWorkingDirectory",
                     "(Lorg/apache/hadoop/fs/Path;)V", jPath) != 0) {
        errno = errnoFromException(jExc, env, "FileSystem::"
                                   "setWorkingDirectory");
        retval = -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return retval;
}



int hdfsCreateDirectory(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  fs.mkdirs(new Path(path));

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Create the directory
    jvalue jVal;
    jVal.z = 0;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "mkdirs", "(Lorg/apache/hadoop/fs/Path;)Z",
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::mkdirs");
        goto done;
    }

 done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return (jVal.z) ? 0 : -1;
}


int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication)
{
    // JAVA EQUIVALENT:
    //  fs.setReplication(new Path(path), replication);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Create the directory
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "setReplication", "(Lorg/apache/hadoop/fs/Path;S)Z",
                     jPath, replication) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::setReplication");
        goto done;
    }

 done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return (jVal.z) ? 0 : -1;
}

int hdfsChown(hdfsFS fs, const char* path, const char *owner, const char *group)
{
    // JAVA EQUIVALENT:
    //  fs.setOwner(path, owner, group)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    if (owner == NULL && group == NULL) {
      fprintf(stderr, "Both owner and group cannot be null in chown");
      errno = EINVAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    jstring jOwnerString = (*env)->NewStringUTF(env, owner);
    jstring jGroupString = (*env)->NewStringUTF(env, group);

    //Create the directory
    int ret = 0;
    jthrowable jExc = NULL;
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "setOwner", JMETHOD3(JPARAM(HADOOP_PATH), JPARAM(JAVA_STRING), JPARAM(JAVA_STRING), JAVA_VOID),
                     jPath, jOwnerString, jGroupString) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::setOwner");
        ret = -1;
        goto done;
    }

 done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jOwnerString);
    destroyLocalReference(env, jGroupString);

    return ret;
}

int hdfsChmod(hdfsFS fs, const char* path, short mode)
{
    // JAVA EQUIVALENT:
    //  fs.setPermission(path, FsPermission)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    // construct jPerm = FsPermission.createImmutable(short mode);

    jshort jmode = mode;

    jobject jPermObj =
      constructNewObjectOfClass(env, NULL, HADOOP_FSPERM,"(S)V",jmode);
    if (jPermObj == NULL) {
      return -2;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
      return -3;
    }

    //Create the directory
    int ret = 0;
    jthrowable jExc = NULL;
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "setPermission", JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_FSPERM), JAVA_VOID),
                     jPath, jPermObj) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::setPermission");
        ret = -1;
        goto done;
    }

 done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPermObj);

    return ret;
}

int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime)
{
    // JAVA EQUIVALENT:
    //  fs.setTimes(src, mtime, atime)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
      fprintf(stderr, "could not construct path object\n");
      return -2;
    }

    jlong jmtime = mtime * (jlong)1000;
    jlong jatime = atime * (jlong)1000;

    int ret = 0;
    jthrowable jExc = NULL;
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "setTimes", JMETHOD3(JPARAM(HADOOP_PATH), "J", "J", JAVA_VOID),
                     jPath, jmtime, jatime) != 0) {
      fprintf(stderr, "call to setTime failed\n");
      errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                 "FileSystem::setTimes");
      ret = -1;
      goto done;
    }

 done:
    destroyLocalReference(env, jPath);
    return ret;
}




char***
hdfsGetHosts(hdfsFS fs, const char* path, tOffset start, tOffset length)
{
    // JAVA EQUIVALENT:
    //  fs.getFileBlockLoctions(new Path(path), start, length);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    jvalue jFSVal;
    jthrowable jFSExc = NULL;
    if (invokeMethod(env, &jFSVal, &jFSExc, INSTANCE, jFS,
                     HADOOP_FS, "getFileStatus",
                     "(Lorg/apache/hadoop/fs/Path;)"
                     "Lorg/apache/hadoop/fs/FileStatus;",
                     jPath) != 0) {
        errno = errnoFromException(jFSExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getFileStatus");
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jobject jFileStatus = jFSVal.l;

    //org.apache.hadoop.fs.FileSystem::getFileBlockLocations
    char*** blockHosts = NULL;
    jobjectArray jBlockLocations;;
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS,
                     HADOOP_FS, "getFileBlockLocations",
                     "(Lorg/apache/hadoop/fs/FileStatus;JJ)"
                     "[Lorg/apache/hadoop/fs/BlockLocation;",
                     jFileStatus, start, length) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getFileBlockLocations");
        destroyLocalReference(env, jPath);
        destroyLocalReference(env, jFileStatus);
        return NULL;
    }
    jBlockLocations = jVal.l;

    //Figure out no of entries in jBlockLocations
    //Allocate memory and add NULL at the end
    jsize jNumFileBlocks = (*env)->GetArrayLength(env, jBlockLocations);

    blockHosts = malloc(sizeof(char**) * (jNumFileBlocks+1));
    if (blockHosts == NULL) {
        errno = ENOMEM;
        goto done;
    }
    blockHosts[jNumFileBlocks] = NULL;
    if (jNumFileBlocks == 0) {
        errno = 0;
        goto done;
    }

    //Now parse each block to get hostnames
    int i = 0;
    for (i=0; i < jNumFileBlocks; ++i) {
        jobject jFileBlock =
            (*env)->GetObjectArrayElement(env, jBlockLocations, i);

        jvalue jVal;
        jobjectArray jFileBlockHosts;
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFileBlock, HADOOP_BLK_LOC,
                         "getHosts", "()[Ljava/lang/String;") ||
                jVal.l == NULL) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "BlockLocation::getHosts");
            destroyLocalReference(env, jPath);
            destroyLocalReference(env, jFileStatus);
            destroyLocalReference(env, jBlockLocations);
            return NULL;
        }

        jFileBlockHosts = jVal.l;
        //Figure out no of hosts in jFileBlockHosts
        //Allocate memory and add NULL at the end
        jsize jNumBlockHosts = (*env)->GetArrayLength(env, jFileBlockHosts);
        blockHosts[i] = malloc(sizeof(char*) * (jNumBlockHosts+1));
        if (blockHosts[i] == NULL) {
            int x = 0;
            for (x=0; x < i; ++x) {
                free(blockHosts[x]);
            }
            free(blockHosts);
            errno = ENOMEM;
            goto done;
        }
        blockHosts[i][jNumBlockHosts] = NULL;

        //Now parse each hostname
        int j = 0;
        const char *hostName;
        for (j=0; j < jNumBlockHosts; ++j) {
            jstring jHost =
                (*env)->GetObjectArrayElement(env, jFileBlockHosts, j);

            hostName =
                (const char*)((*env)->GetStringUTFChars(env, jHost, NULL));
            blockHosts[i][j] = strdup(hostName);

            (*env)->ReleaseStringUTFChars(env, jHost, hostName);
            destroyLocalReference(env, jHost);
        }

        destroyLocalReference(env, jFileBlockHosts);
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jFileStatus);
    destroyLocalReference(env, jBlockLocations);

    return blockHosts;
}


void hdfsFreeHosts(char ***blockHosts)
{
    int i, j;
    for (i=0; blockHosts[i]; i++) {
        for (j=0; blockHosts[i][j]; j++) {
            free(blockHosts[i][j]);
        }
        free(blockHosts[i]);
    }
    free(blockHosts);
}


tOffset hdfsGetDefaultBlockSize(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getDefaultBlockSize();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //FileSystem::getDefaultBlockSize()
    tOffset blockSize = -1;
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "getDefaultBlockSize", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getDefaultBlockSize");
        return -1;
    }
    blockSize = jVal.j;

    return blockSize;
}



tOffset hdfsGetCapacity(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getRawCapacity();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    if (!((*env)->IsInstanceOf(env, jFS,
                               globalClassReference(HADOOP_DFS, env)))) {
        fprintf(stderr, "hdfsGetCapacity works only on a "
                "DistributedFileSystem!\n");
        return -1;
    }

    //FileSystem::getRawCapacity()
    jvalue  jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_DFS,
                     "getRawCapacity", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getRawCapacity");
        return -1;
    }

    return jVal.j;
}



tOffset hdfsGetUsed(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getRawUsed();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    if (!((*env)->IsInstanceOf(env, jFS,
                               globalClassReference(HADOOP_DFS, env)))) {
        fprintf(stderr, "hdfsGetUsed works only on a "
                "DistributedFileSystem!\n");
        return -1;
    }

    //FileSystem::getRawUsed()
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_DFS,
                     "getRawUsed", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getRawUsed");
        return -1;
    }

    return jVal.j;
}



static int
getFileInfoFromStat(JNIEnv *env, jobject jStat, hdfsFileInfo *fileInfo)
{
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                     HADOOP_STAT, "isDir", "()Z") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileStatus::isDir");
        return -1;
    }
    fileInfo->mKind = jVal.z ? kObjectKindDirectory : kObjectKindFile;

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                     HADOOP_STAT, "getReplication", "()S") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileStatus::getReplication");
        return -1;
    }
    fileInfo->mReplication = jVal.s;

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                     HADOOP_STAT, "getBlockSize", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileStatus::getBlockSize");
        return -1;
    }
    fileInfo->mBlockSize = jVal.j;

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                     HADOOP_STAT, "getModificationTime", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileStatus::getModificationTime");
        return -1;
    }
    fileInfo->mLastMod = (tTime) (jVal.j / 1000);

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                     HADOOP_STAT, "getAccessTime", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileStatus::getAccessTime");
        return -1;
    }
    fileInfo->mLastAccess = (tTime) (jVal.j / 1000);


    if (fileInfo->mKind == kObjectKindFile) {
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                         HADOOP_STAT, "getLen", "()J") != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "FileStatus::getLen");
            return -1;
        }
        fileInfo->mSize = jVal.j;
    }

    jobject jPath;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat, HADOOP_STAT,
                     "getPath", "()Lorg/apache/hadoop/fs/Path;") ||
            jVal.l == NULL) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "Path::getPath");
        return -1;
    }
    jPath = jVal.l;

    jstring     jPathName;
    const char *cPathName;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jPath, HADOOP_PATH,
                     "toString", "()Ljava/lang/String;")) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "Path::toString");
        destroyLocalReference(env, jPath);
        return -1;
    }
    jPathName = jVal.l;
    cPathName = (const char*) ((*env)->GetStringUTFChars(env, jPathName, NULL));
    fileInfo->mName = strdup(cPathName);
    (*env)->ReleaseStringUTFChars(env, jPathName, cPathName);
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathName);
    jstring     jUserName;
    const char* cUserName;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat, HADOOP_STAT,
                    "getOwner", "()Ljava/lang/String;")) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileStatus::getOwner failed!\n");
        errno = EINTERNAL;
        return -1;
    }
    jUserName = jVal.l;
    cUserName = (const char*) ((*env)->GetStringUTFChars(env, jUserName, NULL));
    fileInfo->mOwner = strdup(cUserName);
    (*env)->ReleaseStringUTFChars(env, jUserName, cUserName);
    destroyLocalReference(env, jUserName);

    jstring     jGroupName;
    const char* cGroupName;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat, HADOOP_STAT,
                    "getGroup", "()Ljava/lang/String;")) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileStatus::getGroup failed!\n");
        errno = EINTERNAL;
        return -1;
    }
    jGroupName = jVal.l;
    cGroupName = (const char*) ((*env)->GetStringUTFChars(env, jGroupName, NULL));
    fileInfo->mGroup = strdup(cGroupName);
    (*env)->ReleaseStringUTFChars(env, jGroupName, cGroupName);
    destroyLocalReference(env, jGroupName);

    jobject jPermission;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat, HADOOP_STAT,
                     "getPermission", "()Lorg/apache/hadoop/fs/permission/FsPermission;") ||
            jVal.l == NULL) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileStatus::getPermission failed!\n");
        errno = EINTERNAL;
        return -1;
    }
    jPermission = jVal.l;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jPermission, HADOOP_FSPERM,
                         "toShort", "()S") != 0) {
            fprintf(stderr, "Call to org.apache.hadoop.fs.permission."
                    "FsPermission::toShort failed!\n");
            errno = EINTERNAL;
            return -1;
    }
    fileInfo->mPermissions = jVal.s;
    destroyLocalReference(env, jPermission);

    return 0;
}

static int
getFileInfo(JNIEnv *env, jobject jFS, jobject jPath, hdfsFileInfo *fileInfo)
{
    // JAVA EQUIVALENT:
    //  fs.isDirectory(f)
    //  fs.getModificationTime()
    //  fs.getAccessTime()
    //  fs.getLength(f)
    //  f.getPath()
    //  f.getOwner()
    //  f.getGroup()
    //  f.getPermission().toShort()

    jobject jStat;
    jvalue  jVal;
    jthrowable jExc = NULL;

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "exists", JMETHOD1(JPARAM(HADOOP_PATH), "Z"),
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::exists");
        return -1;
    }

    if (jVal.z == 0) {
      errno = ENOENT;
      return -1;
    }

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "getFileStatus", JMETHOD1(JPARAM(HADOOP_PATH), JPARAM(HADOOP_STAT)),
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getFileStatus");
        return -1;
    }
    jStat = jVal.l;

    if(NULL == jStat) {
        return -1;
    }
    int ret =  getFileInfoFromStat(env, jStat, fileInfo);
    destroyLocalReference(env, jStat);
    return ret;
}



hdfsFileInfo* hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries)
{
    // JAVA EQUIVALENT:
    //  Path p(path);
    //  Path []pathList = fs.listPaths(p)
    //  foreach path in pathList
    //    getFileInfo(path)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    hdfsFileInfo *pathList = 0;

    jobjectArray jPathList = NULL;
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_DFS, "listStatus",
                     JMETHOD1(JPARAM(HADOOP_PATH), JARRPARAM(HADOOP_STAT)),
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::listStatus");
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jPathList = jVal.l;

    if(NULL == jPathList) {
        destroyLocalReference(env, jPath);
        return NULL;
    }

    //Figure out no of entries in that directory
    jsize jPathListSize = (*env)->GetArrayLength(env, jPathList);
    *numEntries = jPathListSize;
    if (jPathListSize == 0) {
        errno = 0;
        goto done;
    }

    //Allocate memory
    pathList = calloc(jPathListSize, sizeof(hdfsFileInfo));
    if (pathList == NULL) {
        errno = ENOMEM;
        goto done;
    }

    //Save path information in pathList
    jsize i;
    jobject tmpStat;
    for (i=0; i < jPathListSize; ++i) {
        tmpStat = (*env)->GetObjectArrayElement(env, jPathList, i);
        if (getFileInfoFromStat(env, tmpStat, &pathList[i])) {
            hdfsFreeFileInfo(pathList, jPathListSize);
            destroyLocalReference(env, tmpStat);
            pathList = NULL;
            goto done;
        }
        destroyLocalReference(env, tmpStat);
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathList);

    return pathList;
}

hdfsFileInfo* hdfsGlobDirectory(hdfsFS fs, const char* path, int *numEntries)
{
    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    hdfsFileInfo *pathList = 0;

    jobjectArray jPathList = NULL;
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_DFS, "globStatus",
                     JMETHOD1(JPARAM(HADOOP_PATH), JARRPARAM(HADOOP_STAT)),
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::globStatus");
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jPathList = jVal.l;

    if(NULL == jPathList) {
        destroyLocalReference(env, jPath);
        return NULL;
    }

    //Figure out no of entries in that directory
    jsize jPathListSize = (*env)->GetArrayLength(env, jPathList);
    *numEntries = jPathListSize;
    if (jPathListSize == 0) {
        errno = 0;
        goto done;
    }

    //Allocate memory
    pathList = calloc(jPathListSize, sizeof(hdfsFileInfo));
    if (pathList == NULL) {
        errno = ENOMEM;
        goto done;
    }

    //Save path information in pathList
    jsize i;
    jobject tmpStat;
    for (i=0; i < jPathListSize; ++i) {
        tmpStat = (*env)->GetObjectArrayElement(env, jPathList, i);
        if (getFileInfoFromStat(env, tmpStat, &pathList[i])) {
            hdfsFreeFileInfo(pathList, jPathListSize);
            destroyLocalReference(env, tmpStat);
            pathList = NULL;
            goto done;
        }
        destroyLocalReference(env, tmpStat);
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathList);

    return pathList;
}

hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  File f(path);
    //  fs.isDirectory(f)
    //  fs.lastModified() ??
    //  fs.getLength(f)
    //  f.getPath()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    hdfsFileInfo *fileInfo = calloc(1, sizeof(hdfsFileInfo));
    if (getFileInfo(env, jFS, jPath, fileInfo)) {
        hdfsFreeFileInfo(fileInfo, 1);
        fileInfo = NULL;
        goto done;
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return fileInfo;
}



void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries)
{
    //Free the mName
    int i;
    for (i=0; i < numEntries; ++i) {
        if (hdfsFileInfo[i].mName) {
            free(hdfsFileInfo[i].mName);
            free(hdfsFileInfo[i].mOwner);
            free(hdfsFileInfo[i].mGroup);
        }
    }

    //Free entire block
    free(hdfsFileInfo);
}

// Followed by is Map-Reduce job's query API

/**
 * Helper function to create a org.apache.hadoop.fs.JobClient object.
 * object.
 * @return Returns a jobject on success and NULL on error.
 */
mapredJC JobClientInitialize()
{
    JNIEnv *env = 0;

    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    //Construct a org.apache.hadoop.mapred.JobConf object
    jobject jJobConf =
        constructNewObjectOfClass(env, NULL, "org/apache/hadoop/mapred/JobConf",
                "()V");
    if (jJobConf == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.mapred.JobConf");
        errno = EINTERNAL;
        return NULL;
    }
    //Construct the org.apache.hadoop.fs.Path object
    jobject jJobClient =
        constructNewObjectOfClass(env, NULL, "org/apache/hadoop/mapred/JobClient",
                 "(Lorg/apache/hadoop/mapred/JobConf;)V",
                  jJobConf);
    if (jJobClient == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.mapred.JobClient");
        errno = EINTERNAL;
        return NULL;
    }
    destroyLocalReference(env, jJobConf);

    return jJobClient;
}

int JobClientFinalize(mapredJC jc){
    JNIEnv *env = 0;
    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

   destroyLocalReference(env, jc);
   return 0;
}

MRJobInfo* mapredGetAllJobs(mapredJC jJobClient,int *numItem)
{
    JNIEnv *env = 0;
    jobjectArray  jValArray = NULL;
    jobject jValElement = NULL;
    jthrowable jExc = NULL;
    jvalue jVal;
    jint index;
    const char *str;
    char *str2;

    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      *numItem=-1;
      return NULL;
    }

//    jJobClient = JobClientInitialize(env);


   if (invokeMethod2(env, &jValArray, &jExc, INSTANCE, jJobClient,
              HADOOP_JOBCLIENT, "getAllJobs","()[Lorg/apache/hadoop/mapred/JobStatus;") != 0 ){
      errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred."
                                       "JobClient::getAllJobs");
      *numItem=-1;
      goto done;
   }

   // jobject  GetObjectArrayElement(jobjectArray array, jsize index);
   jsize size = (*env)->GetArrayLength(env, jValArray);
   *numItem = size;
   if(!size){
      return NULL;
   }

   MRJobInfo *ret= malloc(sizeof(MRJobInfo)*size);

   for (index=0; index < size; index++) {
      jValElement = (*env)->GetObjectArrayElement(env,jValArray, index);

      //get Job ID:
      if (invokeMethod(env, &jVal, &jExc, INSTANCE, jValElement,
                 HADOOP_JOBSTATUS, "getJobId","()Ljava/lang/String;") != 0 ){
         errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred."
                                          "JobStatus::getJobId");
         *numItem=-1;
         goto done;
      }

      /* Convert the object just obtained into a String */
      str = (const char*) ((*env)->GetStringUTFChars(env, jVal.l, NULL));
      strncpy(ret[index].jobID,str,30);
      /* Free up memory to prevent memory leaks */
      (*env)->ReleaseStringUTFChars(env, jVal.l, str);

      //get User Name:
      if (invokeMethod(env, &jVal, &jExc, INSTANCE, jValElement,
                 HADOOP_JOBSTATUS, "getUsername","()Ljava/lang/String;") != 0 ){
         errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred."
                                          "JobStatus::getUsername");
         *numItem=-1;
         goto done;
      }

      /* Convert the object just obtained into a String */
      str = (const char*) ((*env)->GetStringUTFChars(env, jVal.l, NULL));
      strncpy(ret[index].user,str,30);
      /* Free up memory to prevent memory leaks */
      (*env)->ReleaseStringUTFChars(env, jVal.l, str);

      //get Start Time:
      if (invokeMethod(env, &jVal, &jExc, INSTANCE, jValElement,
                 HADOOP_JOBSTATUS, "getStartTime","()J") != 0 ){
         errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred."
                                          "JobStatus::getStartTime");
         *numItem=-1;
         goto done;
      }


      ret[index].startTime = jVal.j;
      str2=ctime(&ret[index].startTime);

      //get runState:
      if (invokeMethod(env, &jVal, &jExc, INSTANCE, jValElement,
                 HADOOP_JOBSTATUS, "getRunState","()I") != 0 ){
         errno = errnoFromException(jExc, env, "org.apache.hadoop.mapred."
                                          "JobStatus::getRunState");
         *numItem=-1;
         goto done;
      }

      ret[index].runState = jVal.i;

   }

  done:

   // Release unnecessary local references
   //destroyLocalReference(env, jJobClient);
   return ret;

}

MRJobInfo* mapredGetJob(mapredJC jJobClient, const char *jobid)
{
    int index,num;
    MRJobInfo* ret= malloc(sizeof(MRJobInfo));
    MRJobInfo* t=NULL;
    int found=0;

    t=mapredGetAllJobs(jJobClient,&num);
    for (index=0; index<num; index++){
       if(strcmp(t[index].jobID,jobid))
           continue;
       *ret = t[index];
       found=1;
       break;
    }

   // Release unnecessary local references
   free(t);

   if(found)
     return ret;
   else return NULL;

}




/**
 * vim: ts=4: sw=4: et:
 */

