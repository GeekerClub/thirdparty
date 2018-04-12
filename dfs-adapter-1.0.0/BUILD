# libhdfs Depends on libhdf.a in LD_LIBRARY_PATH.
cc_library(
    name = 'jvm',
    prebuilt = True,
)

cc_library(
    name = 'hdfs',
    deps = [
        ':jvm'
    ],
    prebuilt = True
)

#check libjvm.so
def set_env():
    import os, sys
    runtime_path = os.getcwd()
    output = os.popen('. ' + runtime_path + '/deploy/onebox/env.sh && echo $LD_LIBRARY_PATH')
    lines = output.read().strip().split('\n')
    if (len(lines) > 1):
        os.environ['LD_LIBRARY_PATH'] = lines[len(lines) - 1]

set_env()
