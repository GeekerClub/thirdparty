proto_library(
        name = 'hyperloglog_proto',
        srcs = 'hyperloglog.proto'
)

cc_library(
    name = 'hyperloglog',
    srcs = [
        'hyperloglog.cc',
    ],
    deps = [
        ':hyperloglog_proto',
        ':sds'
    ],
)

cc_test(
    name = 'hyperloglog_test',
    srcs = 'hyperloglog_test.cc',
    deps = ':hyperloglog'
)

cc_library(
    name = 'sds',
    srcs = [
        'zmalloc.cc',
        'sds.cc'
    ],
    deps = [
        '//thirdparty/glog:glog',    
    ]
)
