// Copyright (C) 2017, For toft authors.
// Author: Pengyu Sun (sunpengyu@nbjl.nankai.edu.cn)
//
// Description:

#include <string>

#include "thirdparty/gmock/gmock.h"
#include "thirdparty/gtest/gtest.h"

#include "thirdparty/redis-hyperloglog/hyperloglog.h"

namespace hll {

using ::testing::_;
using ::testing::Return;
using ::testing::NiceMock;

TEST(HyperLogLogTest, multi_test) {
    robj *redis_object = hllCreate();
    int64_t num = 0;
    num = hllCount((hllhdr*)redis_object->ptr, NULL);
    EXPECT_EQ(0, num);

    std::string key = "0";
    hllAdd(redis_object, key);
    num = hllCount((hllhdr*)redis_object->ptr, NULL);
    EXPECT_EQ(1, num);

    hllAdd(redis_object, key);
    num = hllCount((hllhdr*)redis_object->ptr, NULL);
    EXPECT_EQ(1, num);

    robj *merge = hllCreate();
    for (int i =0; i < 10; ++i) {
        key = key + 'a';
        hllAdd(merge, key);
    }
    num = hllCount((hllhdr*)merge->ptr, NULL);
    EXPECT_EQ(10, num);

    Merge(merge, redis_object);
    num = hllCount((hllhdr*)merge->ptr, NULL);
    EXPECT_EQ(11, num);

    std::string str = hllSerializeToPBString(*((hllhdr*)merge->ptr));
    robj *parse = hllCreate();

    ParseHLLFromString((hllhdr*)parse->ptr, str);
    num = hllCount((hllhdr*)parse->ptr, NULL);
    EXPECT_EQ(11, num);

    hllFreeStringObject(redis_object);
    hllFreeStringObject(merge);
    hllFreeStringObject(parse);

}
}
