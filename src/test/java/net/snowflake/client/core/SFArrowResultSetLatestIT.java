/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import net.snowflake.client.category.TestCategoryArrow;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1;
import net.snowflake.client.jdbc.telemetry.NoOpTelemetryClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryArrow.class)
public class SFArrowResultSetLatestIT extends SFArrowResultSetIT {

  /** Test that first chunk containing struct vectors (used for timestamps) can be sorted */
  @Test
  public void testSortedResultChunkWithStructVectors() throws Throwable {
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "TIMESTAMP_LTZ");
    customFieldMeta.put("scale", "38");
    // test normal date
    FieldType fieldType =
        new FieldType(true, Types.MinorType.BIGINT.getType(), null, customFieldMeta);
    FieldType fieldType2 =
        new FieldType(true, Types.MinorType.INT.getType(), null, customFieldMeta);

    StructVector structVector = StructVector.empty("testListVector", allocator);
    List<Field> fieldList = new LinkedList<Field>();
    Field bigIntField = new Field("epoch", fieldType, null);

    Field intField = new Field("fraction", fieldType2, null);

    fieldList.add(bigIntField);
    fieldList.add(intField);

    FieldType structFieldType =
        new FieldType(true, Types.MinorType.STRUCT.getType(), null, customFieldMeta);
    Field structField = new Field("timestamp", structFieldType, fieldList);

    structVector.initializeChildrenFromFields(fieldList);

    List<Field> fieldListMajor = new LinkedList<Field>();
    fieldListMajor.add(structField);
    Schema dataSchema = new Schema(fieldList);
    Object[][] data = generateData(dataSchema, 1000);

    Schema schema = new Schema(fieldListMajor);

    File file = createArrowFile("testTimestamp", schema, data, 10);

    int dataSize = (int) file.length();
    byte[] dataBytes = new byte[dataSize];

    InputStream is = new FileInputStream(file);
    is.read(dataBytes, 0, dataSize);

    SnowflakeResultSetSerializableV1 resultSetSerializable = new SnowflakeResultSetSerializableV1();
    resultSetSerializable.setRootAllocator(new RootAllocator(Long.MAX_VALUE));
    resultSetSerializable.setFirstChunkStringData(Base64.getEncoder().encodeToString(dataBytes));
    resultSetSerializable.setFirstChunkByteData(dataBytes);
    resultSetSerializable.setChunkFileCount(0);
    resultSetSerializable.setTimestampLTZFormatter("YYYY-MM-DD HH24:MI:SS.FFTZH:TZM");

    SFArrowResultSet resultSet =
        new SFArrowResultSet(resultSetSerializable, new NoOpTelemetryClient(), true);
    try {
      for (int i = 0; i < 1000; i++) {
        resultSet.next();
      }
    } catch (Exception ex) {
      // Since we have no way of formatting timestamps from here since we have no metadata, expect
      // an exception
    }
  }

  /** Test that the first chunk can be sorted */
  @Test
  public void testSortedResultChunk() throws Throwable {
    List<Field> fieldList = new ArrayList<>();
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("scale", "0");
    FieldType type = new FieldType(false, Types.MinorType.INT.getType(), null, customFieldMeta);
    fieldList.add(new Field("", type, null));

    customFieldMeta.put("logicalType", "DATE");
    type = new FieldType(false, Types.MinorType.DATEDAY.getType(), null, customFieldMeta);
    fieldList.add(new Field("", type, null));

    customFieldMeta.put("logicalType", "FIXED");
    type = new FieldType(false, Types.MinorType.BIGINT.getType(), null, customFieldMeta);
    fieldList.add(new Field("", type, null));

    customFieldMeta.put("logicalType", "REAL");
    type = new FieldType(false, Types.MinorType.FLOAT8.getType(), null, customFieldMeta);
    fieldList.add(new Field("", type, null));

    customFieldMeta.put("logicalType", "FIXED");
    type = new FieldType(false, Types.MinorType.SMALLINT.getType(), null, customFieldMeta);
    fieldList.add(new Field("", type, null));

    customFieldMeta.put("logicalType", "FIXED");
    type = new FieldType(false, Types.MinorType.TINYINT.getType(), null, customFieldMeta);
    fieldList.add(new Field("", type, null));

    customFieldMeta.put("logicalType", "BINARY");
    type = new FieldType(false, Types.MinorType.VARBINARY.getType(), null, customFieldMeta);
    fieldList.add(new Field("", type, null));

    customFieldMeta.put("logicalType", "TEXT");
    type = new FieldType(false, Types.MinorType.VARCHAR.getType(), null, customFieldMeta);
    fieldList.add(new Field("", type, null));

    customFieldMeta.put("logicalType", "BOOLEAN");
    type = new FieldType(false, Types.MinorType.BIT.getType(), null, customFieldMeta);
    fieldList.add(new Field("", type, null));

    customFieldMeta.put("logicalType", "REAL");
    type = new FieldType(false, new ArrowType.Decimal(38, 16, 128), null, customFieldMeta);
    fieldList.add(new Field("", type, null));

    Schema schema = new Schema(fieldList);

    Object[][] data = generateData(schema, 1000);
    File file = createArrowFile("testNoOfflineData_0_0_0", schema, data, 10);

    int dataSize = (int) file.length();
    byte[] dataBytes = new byte[dataSize];

    InputStream is = new FileInputStream(file);
    is.read(dataBytes, 0, dataSize);

    SnowflakeResultSetSerializableV1 resultSetSerializable = new SnowflakeResultSetSerializableV1();
    resultSetSerializable.setRootAllocator(new RootAllocator(Long.MAX_VALUE));
    resultSetSerializable.setFirstChunkStringData(Base64.getEncoder().encodeToString(dataBytes));
    resultSetSerializable.setFirstChunkByteData(dataBytes);
    resultSetSerializable.setChunkFileCount(0);
    resultSetSerializable.setDateFormatter("YYYY-MM-DD");

    SFArrowResultSet resultSet =
        new SFArrowResultSet(resultSetSerializable, new NoOpTelemetryClient(), true);

    for (int i = 0; i < 1000; i++) {
      resultSet.next();
    }
    // Since we sorted the list, the last result should be null
    assertEquals(null, resultSet.getObject(1));
    assertFalse(resultSet.next());
  }
}
