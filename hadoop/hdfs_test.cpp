// Copyright 2013, For toft authors.
// Author: An Qin (anqin.qin@gmail.com)

#include "thirdparty/hadoop/hdfs.h"

#include <string>

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/gtest/gtest.h"

DEFINE_string(name_node, "testbed.aliyun.com", "hdfs cluster namenode name.");
DEFINE_int32(port, 54310, "hdfs cluster namenode name.");
DEFINE_string(user, "logging", "hdfs access user name.");
DEFINE_string(passwd, "logging", "hdfs access password.");
DEFINE_string(file_name, "/tmp/hanchao/test_sync1", "one file full path name in hdfs.");
// DEFINE_string(file_name, "/app/dt/logging/tmp/hanchao/test_sync", "one file full path name in hdfs.");


class HdfsStoreTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_fs = hdfsConnectAsUser(FLAGS_name_node.c_str(), FLAGS_port,
                                 FLAGS_user.c_str(), FLAGS_passwd.c_str());
        ASSERT_TRUE(NULL != m_fs);
        // if file exists remove it.
        if (0 == hdfsExists(m_fs, FLAGS_file_name.c_str())) {
            ASSERT_EQ(0, hdfsDelete(m_fs, FLAGS_file_name.c_str()));
        }
    }

    virtual void TearDown() {
        if (NULL != m_fs) {
            hdfsDisconnect(m_fs);
            m_fs = NULL;
        }
    }

    hdfsFS m_fs;
};

// reader thread.
TEST_F(HdfsStoreTest, Case) {
    // open one file.
    hdfsFile file = hdfsOpenFile(m_fs, FLAGS_file_name.c_str(), O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file);

    // write a string.
    std::string content = "This is test data =";
    content.append("1").append(" ");

    tSize write_size = hdfsWrite(m_fs, file, content.c_str(), content.size());
    ASSERT_EQ(write_size, content.size());

    // read can success, but file size is 0.
    hdfsFile file_ro = hdfsOpenFile(m_fs, FLAGS_file_name.c_str(), O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file_ro);

    ASSERT_EQ(-1, hdfsSeek(m_fs, file_ro, 2));

    ASSERT_EQ(0, hdfsCloseFile(m_fs, file_ro));

    // sync
    ASSERT_EQ(0, hdfsSync(m_fs, file));

    file_ro = hdfsOpenFile(m_fs, FLAGS_file_name.c_str(), O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file_ro);

    char buffer[1024] = {0};

//     tSize read_size = hdfsRead(m_fs, file_ro, buffer, content.size());
    hdfsRead(m_fs, file_ro, buffer, content.size());

    ASSERT_STREQ(buffer, content.c_str());

    ASSERT_EQ(0, hdfsCloseFile(m_fs, file_ro));

    ASSERT_EQ(0, hdfsCloseFile(m_fs, file));

    // truncate
    ASSERT_EQ(0, hdfsTruncate(m_fs, FLAGS_file_name.c_str(), 3));


    // reopen and write.
    file = hdfsOpenFile(m_fs, FLAGS_file_name.c_str(), O_APPEND | O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file);

    write_size = hdfsWrite(m_fs, file, content.c_str(), content.size());
    ASSERT_EQ(write_size, content.size());

    ASSERT_EQ(0, hdfsCloseFile(m_fs, file));
}


int main(int argc, char* argv[]) {
    ::google::InitGoogleLogging(argv[0]);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
