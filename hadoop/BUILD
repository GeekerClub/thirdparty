
cc_library(
    name = 'jvm',
    prebuilt = True,
)

cc_library(
    name = 'hdfs',
    prebuilt = True,
)

cc_library(
    name = 'hdfs_jni',
    srcs = [
        'hdfs.c',
        'hdfsJniHelper.c',
    ],
    deps = [
        ':jvm'
    ],
    #prebuilt = True,
)

cc_test(
    name = 'hdfs_test',
    srcs = 'hdfs_test.cpp',
    deps = [
        ':hdfs_jni',
    ],
    
    #link_all_symbols = True,
)
