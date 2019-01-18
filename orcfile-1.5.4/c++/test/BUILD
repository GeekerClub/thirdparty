cc_library(
    name = 'memory_io_stream',
    srcs = [
           'MemoryOutputStream.cc',
           'MemoryInputStream.cc'
    ],
    deps = [
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
    incs = ['../include', '../src']
)


cc_test(
   name = 'test_buffer_stream',
   srcs = [
           'TestBufferedOutputStream.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)

cc_test(
   name = 'test_writer',
   srcs = [
           'TestWriter.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)


cc_test(
   name = 'test_reader',
   srcs = [
           'TestReader.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)



cc_test(
   name = 'test_column_reader',
   srcs = [
           'TestColumnReader.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '//thirdparty/gmock:gmock',
           '#lz4'
    ],
   incs = ['../include', '../src']
)


cc_test(
   name = 'test_column_printer',
   srcs = [
           'TestColumnPrinter.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)


cc_test(
   name = 'test_column_statistics',
   srcs = [
           'TestColumnStatistics.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)


# cc_test(
#    name = 'test_stripe_index_statistics',
#    srcs = [
#            'TestStripeIndexStatistics.cc',
#     ],
#    deps = [
#            ':memory_io_stream',
#            '//:orc',
#            '//thirdparty/protobuf:protobuf',
#            '//thirdparty/gtest:gtest',
#            '//thirdparty/snappy:snappy',
#            '#lz4'
#     ],
#    incs = ['../include', '../src']
# )


cc_test(
   name = 'test_rle',
   srcs = [
           'TestRle.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)

cc_test(
   name = 'test_byterle',
   srcs = [
           'TestByteRle.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)

cc_test(
   name = 'test_compression',
   srcs = [
           'TestCompression.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)


cc_test(
   name = 'test_decompression',
   srcs = [
           'TestDecompression.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)



cc_test(
   name = 'test_type',
   srcs = [
           'TestType.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)


cc_test(
   name = 'test_timezone',
   srcs = [
           'TestTimezone.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)


cc_test(
   name = 'test_driver',
   srcs = [
           'TestDriver.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)


cc_test(
   name = 'test_decimal',
   srcs = [
           'TestDecimal.cc',
    ],
   deps = [
           ':memory_io_stream',
           '//:orc',
           '//thirdparty/protobuf:protobuf',
           '//thirdparty/gtest:gtest',
           '//thirdparty/snappy:snappy',
           '#lz4'
    ],
   incs = ['../include', '../src']
)
