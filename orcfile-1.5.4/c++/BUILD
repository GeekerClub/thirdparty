proto_library(
    name = 'orc_proto',
    srcs = 'src/orc_proto.proto'
)

cc_library(
    name = 'ocr',
    srcs = [
        'src/Writer.cc',
        'src/Common.cc',
        'src/OrcFile.cc',
#         'src/OrcHdfsFile.cc',
        'src/Timezone.cc',
        'src/Statistics.cc',
        'src/Reader.cc',
        'src/RLEv1.cc',
        'src/ColumnReader.cc',
        'src/LzoDecompressor.cc',
        'src/Compression.cc',
        'src/ByteRLE.cc',
        'src/io/InputStream.cc',
        'src/io/OutputStream.cc',
        'src/StripeStream.cc',
        'src/Vector.cc',
        'src/RLE.cc',
        'src/RLEv2.cc',
        'src/ColumnPrinter.cc',
        'src/Int128.cc',
        'src/Exceptions.cc',
        'src/wrap/orc-proto-wrapper.cc',
        'src/ColumnWriter.cc',
        'src/Adaptor.cc',
        'src/TypeImpl.cc',
        'src/MemoryPool.cc',
    ],
    deps = [
        ':orc_proto',
        '//thirdparty/protobuf:protobuf'
    ],
    incs = ['include', 'src']
)
