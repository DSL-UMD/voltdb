/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <vector>

#include "harness.h"
#include "common/TupleSchema.h"
#include "common/TupleSchemaBuilder.h"
#include "test_utils/ScopedTupleSchema.hpp"

using voltdb::TupleSchema;
using voltdb::ValueType;
using voltdb::VALUE_TYPE_BIGINT;
using voltdb::VALUE_TYPE_DECIMAL;
using voltdb::VALUE_TYPE_INTEGER;
using voltdb::VALUE_TYPE_TIMESTAMP;
using voltdb::VALUE_TYPE_VARCHAR;
using voltdb::VALUE_TYPE_TINYINT;

class TupleSchemaTest : public Test
{
};

TEST_F(TupleSchemaTest, Basic)
{
    voltdb::TupleSchemaBuilder builder(2);

    builder.setColumnAtIndex(0, VALUE_TYPE_INTEGER);
    builder.setColumnAtIndex(1, VALUE_TYPE_VARCHAR,
                             256, // column size
                             false, // do not allow nulls
                             true); // size is in bytes

    ScopedTupleSchema schema(builder.build());

    ASSERT_NE(NULL, schema.get());
    ASSERT_EQ(2, schema->columnCount());

    EXPECT_EQ(1, schema->getUninlinedObjectColumnCount());
    EXPECT_EQ(1, schema->getUninlinedObjectColumnInfoIndex(0));

    // 4 bytes for the integer
    // 8 bytes for the string pointer
    EXPECT_EQ(12, schema->tupleLength());

    const TupleSchema::ColumnInfo *colInfo = schema->getColumnInfo(0);
    ASSERT_NE(NULL, colInfo);
    EXPECT_EQ(0, colInfo->offset);
    EXPECT_EQ(4, colInfo->length);
    EXPECT_EQ(VALUE_TYPE_INTEGER, colInfo->type);
    EXPECT_EQ(1, colInfo->allowNull);
    EXPECT_EQ(true, colInfo->inlined);
    EXPECT_EQ(false, colInfo->inBytes);

    colInfo = schema->getColumnInfo(1);
    ASSERT_NE(NULL, colInfo);
    EXPECT_EQ(4, colInfo->offset);
    EXPECT_EQ(256, colInfo->length);
    EXPECT_EQ(VALUE_TYPE_VARCHAR, colInfo->type);
    EXPECT_EQ(false, colInfo->allowNull);
    EXPECT_EQ(false, colInfo->inlined);
    EXPECT_EQ(true, colInfo->inBytes);
}

TEST_F(TupleSchemaTest, HiddenColumn)
{
    voltdb::TupleSchemaBuilder builder(2,  // 2 visible columns
                                       2); // 1 hidden column
    builder.setColumnAtIndex(0, VALUE_TYPE_INTEGER);
    builder.setColumnAtIndex(1, VALUE_TYPE_VARCHAR,
                             256,   // column size
                             false, // do not allow nulls
                             true); // size is in bytes

    builder.setHiddenColumnAtIndex(0, VALUE_TYPE_BIGINT);
    builder.setHiddenColumnAtIndex(1, VALUE_TYPE_TINYINT);
    ScopedTupleSchema schema(builder.build());

    ASSERT_NE(NULL, schema.get());
    ASSERT_EQ(2, schema->columnCount());
    ASSERT_EQ(2, schema->hiddenColumnCount());

    EXPECT_EQ(1, schema->getUninlinedObjectColumnCount());
    EXPECT_EQ(1, schema->getUninlinedObjectColumnInfoIndex(0));

    // 8 bytes for the hidden bigint
    // 4 bytes for the integer
    // 8 bytes for the string pointer
    // 1 byte  for the hidden tinyint
    EXPECT_EQ(21, schema->tupleLength());

    EXPECT_EQ(0, schema->getUninlinedObjectHiddenColumnCount());
    EXPECT_EQ(12, schema->offsetOfHiddenColumns());
    EXPECT_EQ(9, schema->lengthOfAllHiddenColumns());

    // Verify that the visible columns are as expected
    const TupleSchema::ColumnInfo *colInfo = schema->getColumnInfo(0);
    ASSERT_NE(NULL, colInfo);
    EXPECT_EQ(0, colInfo->offset);
    EXPECT_EQ(4, colInfo->length);
    EXPECT_EQ(VALUE_TYPE_INTEGER, colInfo->type);
    EXPECT_EQ(1, colInfo->allowNull);
    EXPECT_EQ(true, colInfo->inlined);
    EXPECT_EQ(false, colInfo->inBytes);

    colInfo = schema->getColumnInfo(1);
    ASSERT_NE(NULL, colInfo);
    EXPECT_EQ(4, colInfo->offset);
    EXPECT_EQ(256, colInfo->length);
    EXPECT_EQ(VALUE_TYPE_VARCHAR, colInfo->type);
    EXPECT_EQ(false, colInfo->allowNull);
    EXPECT_EQ(false, colInfo->inlined);
    EXPECT_EQ(true, colInfo->inBytes);

    // Now check the hidden column
    colInfo = schema->getHiddenColumnInfo(0);
    ASSERT_NE(NULL, colInfo);
    EXPECT_EQ(12, colInfo->offset);
    EXPECT_EQ(8, colInfo->length);
    EXPECT_EQ(VALUE_TYPE_BIGINT, colInfo->type);
    EXPECT_EQ(true, colInfo->allowNull);
    EXPECT_EQ(true, colInfo->inlined);
    EXPECT_EQ(false, colInfo->inBytes);

    colInfo = schema->getHiddenColumnInfo(1);
       ASSERT_NE(NULL, colInfo);
       EXPECT_EQ(20, colInfo->offset);
       EXPECT_EQ(1, colInfo->length);
       EXPECT_EQ(VALUE_TYPE_TINYINT, colInfo->type);
       EXPECT_EQ(true, colInfo->allowNull);
       EXPECT_EQ(true, colInfo->inlined);
       EXPECT_EQ(false, colInfo->inBytes);
}

TEST_F(TupleSchemaTest, EqualsAndCompatibleForMemcpy)
{
    voltdb::TupleSchemaBuilder builder(3); // 3 visible columns
    builder.setColumnAtIndex(0, VALUE_TYPE_DECIMAL);
    builder.setColumnAtIndex(1, VALUE_TYPE_VARCHAR,
                             64,     // length
                             true,   // allow nulls
                             false); // length not in bytes
    builder.setColumnAtIndex(2, VALUE_TYPE_TIMESTAMP);
    ScopedTupleSchema schema1(builder.build());

    voltdb::TupleSchemaBuilder hiddenBuilder(3, 2); // 3 visible columns
    hiddenBuilder.setColumnAtIndex(0, VALUE_TYPE_DECIMAL);
    hiddenBuilder.setColumnAtIndex(1, VALUE_TYPE_VARCHAR,
                             64,     // length
                             true,   // allow nulls
                             false); // length not in bytes
    hiddenBuilder.setColumnAtIndex(2, VALUE_TYPE_TIMESTAMP);

    hiddenBuilder.setHiddenColumnAtIndex(0, VALUE_TYPE_BIGINT);
    hiddenBuilder.setHiddenColumnAtIndex(1, VALUE_TYPE_VARCHAR, 10);

    ScopedTupleSchema schema2(hiddenBuilder.build());

    ASSERT_NE(NULL, schema1.get());
    ASSERT_NE(NULL, schema2.get());

    // Table tuples whose schemas that differ only in hidden columns
    // are not suitable for memcpy.
    EXPECT_FALSE(schema1->isCompatibleForMemcpy(schema2.get()));
    EXPECT_FALSE(schema2->isCompatibleForMemcpy(schema1.get()));
    EXPECT_FALSE(schema1->equals(schema2.get()));
    EXPECT_FALSE(schema2->equals(schema1.get()));

    // Create another schema where the varchar column is longer (but
    // still uninlined)
    builder.setColumnAtIndex(1, VALUE_TYPE_VARCHAR, 128);
    ScopedTupleSchema schema3(builder.build());

    // Structural layout is the same
    EXPECT_TRUE(schema1->isCompatibleForMemcpy(schema3.get()));
    EXPECT_TRUE(schema3->isCompatibleForMemcpy(schema1.get()));

    // But schemas are not equal due to length difference
    EXPECT_FALSE(schema1->equals(schema3.get()));
    EXPECT_FALSE(schema3->equals(schema1.get()));

    // Now do a similar test comparing two schemas with hidden columns.
    hiddenBuilder.setHiddenColumnAtIndex(0,
                                         VALUE_TYPE_BIGINT,
                                         8,
                                         false); // nulls not allowed
    ScopedTupleSchema schema4(hiddenBuilder.build());

    // Structural layout is the same
    EXPECT_TRUE(schema2->isCompatibleForMemcpy(schema4.get()));
    EXPECT_TRUE(schema4->isCompatibleForMemcpy(schema2.get()));

    // But schemas are not equal due to difference in nullability in
    // first hidden column.
    EXPECT_FALSE(schema2->equals(schema4.get()));
    EXPECT_FALSE(schema4->equals(schema2.get()));
}


TEST_F(TupleSchemaTest, MaxSerializedTupleSize) {
    voltdb::TupleSchemaBuilder builder(3); // 3 visible columns
    builder.setColumnAtIndex(0, VALUE_TYPE_DECIMAL);
    builder.setColumnAtIndex(1, VALUE_TYPE_VARCHAR,
                             64,     // length
                             true,   // allow nulls
                             false); // length not in bytes
    builder.setColumnAtIndex(2, VALUE_TYPE_TIMESTAMP);
    ScopedTupleSchema schema(builder.build());

    EXPECT_EQ((4 + 16 + (4 + 64 * 4) + 8),
              schema.get()->getMaxSerializedTupleSize());

    voltdb::TupleSchemaBuilder hiddenBuilder(3, 2); // 3 visible columns, 2 hidden columns
    hiddenBuilder.setColumnAtIndex(0, VALUE_TYPE_DECIMAL);
    hiddenBuilder.setColumnAtIndex(1, VALUE_TYPE_VARCHAR,
                             64,     // length
                             true,   // allow nulls
                             false); // length not in bytes
    hiddenBuilder.setColumnAtIndex(2, VALUE_TYPE_TIMESTAMP);
    hiddenBuilder.setHiddenColumnAtIndex(0, VALUE_TYPE_BIGINT);
    hiddenBuilder.setHiddenColumnAtIndex(1, VALUE_TYPE_VARCHAR, 10, true, true);
    ScopedTupleSchema schemaWithHidden(hiddenBuilder.build());

    EXPECT_EQ((4 + 16 + (4 + 64 * 4) + 8 + 8 + (4 + 10)),
              schemaWithHidden.get()->getMaxSerializedTupleSize(true));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
