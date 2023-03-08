/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import net.snowflake.client.category.TestCategoryArrow;
import net.snowflake.client.jdbc.*;
import net.snowflake.client.jdbc.telemetry.NoOpTelemetryClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

@Category(TestCategoryArrow.class)
public class SFArrowResultSetIT {
  private Random random = new Random();

  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  /** temporary folder to store result files */
  @Rule public TemporaryFolder resultFolder = new TemporaryFolder();

  /** Test the case that all results are returned in first chunk */
  @Test
  public void testNoOfflineData() throws Throwable {
    List<Field> fieldList = new ArrayList<>();
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("scale", "0");
    FieldType type = new FieldType(false, Types.MinorType.INT.getType(), null, customFieldMeta);
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

    SFArrowResultSet resultSet =
        new SFArrowResultSet(resultSetSerializable, new NoOpTelemetryClient(), false);

    int i = 0;
    while (resultSet.next()) {
      int val = resultSet.getInt(1);
      assertThat(val, equalTo(data[0][i]));
      i++;
    }

    // assert that total rowcount is 1000
    assertThat(i, is(1000));
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
    customFieldMeta.put("logicalType", "REAL");
    type = new FieldType(false, Types.MinorType.BIGINT.getType(), null, customFieldMeta);
    fieldList.add(new Field("", type, null));
    customFieldMeta.put("logicalType", "REAL");
    type = new FieldType(false, Types.MinorType.DECIMAL.getType(), null, customFieldMeta);
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

    SFArrowResultSet resultSet =
        new SFArrowResultSet(resultSetSerializable, new NoOpTelemetryClient(), true);

    int i = 0;
    while (resultSet.next()) {
      int val = resultSet.getInt(1);
      assertThat(val, equalTo(data[0][i]));
      i++;
    }

    // assert that total rowcount is 1000
    assertThat(i, is(1000));
  }

  @Test
  public void testEmptyResultSet() throws Throwable {
    SnowflakeResultSetSerializableV1 resultSetSerializable = new SnowflakeResultSetSerializableV1();
    resultSetSerializable.setFirstChunkStringData(
        Base64.getEncoder().encodeToString("".getBytes(StandardCharsets.UTF_8)));
    resultSetSerializable.setChunkFileCount(0);

    SFArrowResultSet resultSet =
        new SFArrowResultSet(resultSetSerializable, new NoOpTelemetryClient(), false);
    assertThat(resultSet.next(), is(false));
    assertThat(resultSet.isLast(), is(false));
    assertThat(resultSet.isAfterLast(), is(true));

    resultSetSerializable.setFirstChunkStringData(null);
    resultSet = new SFArrowResultSet(resultSetSerializable, new NoOpTelemetryClient(), false);

    assertThat(resultSet.next(), is(false));
    assertThat(resultSet.isLast(), is(false));
    assertThat(resultSet.isAfterLast(), is(true));
  }

  /** Testing the case that all data comes from chunk downloader */
  @Test
  public void testOnlyOfflineData() throws Throwable {
    final int colCount = 2;
    final int chunkCount = 10;

    // generate data
    List<Field> fieldList = new ArrayList<>();
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("scale", "0");
    FieldType type = new FieldType(false, Types.MinorType.INT.getType(), null, customFieldMeta);

    for (int i = 0; i < colCount; i++) {
      fieldList.add(new Field("col_" + i, type, null));
    }
    Schema schema = new Schema(fieldList);

    // generate 10 chunk of data
    List<Object[][]> dataLists = new ArrayList<>();
    List<File> fileLists = new ArrayList<>();
    for (int i = 0; i < chunkCount; i++) {
      Object[][] data = generateData(schema, 500);
      File file = createArrowFile("testOnlyOfflineData_" + i, schema, data, 10);
      dataLists.add(data);
      fileLists.add(file);
    }

    SnowflakeResultSetSerializableV1 resultSetSerializable = new SnowflakeResultSetSerializableV1();
    resultSetSerializable.setChunkDownloader(new MockChunkDownloader(fileLists));
    resultSetSerializable.setChunkFileCount(chunkCount);

    SFArrowResultSet resultSet =
        new SFArrowResultSet(resultSetSerializable, new NoOpTelemetryClient(), false);

    int index = 0;
    while (resultSet.next()) {
      for (int i = 0; i < colCount; i++) {
        int val = resultSet.getInt(i + 1);
        Integer expectedVal = (Integer) dataLists.get(index / 500)[i][index % 500];
        assertThat(val, is(expectedVal));
      }
      index++;
    }

    // assert that total rowcount is 5000
    assertThat(index, is(5000));
  }

  /** Testing the case that all data comes from chunk downloader */
  @Test
  public void testFirstResponseAndOfflineData() throws Throwable {
    final int colCount = 2;
    final int chunkCount = 10;

    // generate data
    List<Field> fieldList = new ArrayList<>();
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("scale", "0");
    FieldType type = new FieldType(false, Types.MinorType.INT.getType(), null, customFieldMeta);

    for (int i = 0; i < colCount; i++) {
      fieldList.add(new Field("col_" + i, type, null));
    }
    Schema schema = new Schema(fieldList);

    // generate 10 chunk of data
    List<Object[][]> dataLists = new ArrayList<>();
    List<File> fileLists = new ArrayList<>();

    // first chunk set to base64 rowset
    Object[][] firstChunkData = generateData(schema, 500);
    File arrowFile = createArrowFile("testOnlyOfflineData_0", schema, firstChunkData, 10);

    dataLists.add(firstChunkData);

    int dataSize = (int) arrowFile.length();
    byte[] dataBytes = new byte[dataSize];

    InputStream is = new FileInputStream(arrowFile);
    is.read(dataBytes, 0, dataSize);

    SnowflakeResultSetSerializableV1 resultSetSerializable = new SnowflakeResultSetSerializableV1();
    resultSetSerializable.setFirstChunkStringData(Base64.getEncoder().encodeToString(dataBytes));
    resultSetSerializable.setFirstChunkByteData(dataBytes);
    resultSetSerializable.setChunkFileCount(chunkCount);
    resultSetSerializable.setRootAllocator(new RootAllocator(Long.MAX_VALUE));
    // build chunk downloader
    for (int i = 0; i < chunkCount; i++) {
      Object[][] data = generateData(schema, 500);
      File file = createArrowFile("testOnlyOfflineData_" + (i + 1), schema, data, 10);
      dataLists.add(data);
      fileLists.add(file);
    }
    resultSetSerializable.setChunkDownloader(new MockChunkDownloader(fileLists));

    SFArrowResultSet resultSet =
        new SFArrowResultSet(resultSetSerializable, new NoOpTelemetryClient(), false);

    int index = 0;
    while (resultSet.next()) {
      for (int i = 0; i < colCount; i++) {
        int val = resultSet.getInt(i + 1);
        Integer expectedVal = (Integer) dataLists.get(index / 500)[i][index % 500];
        assertThat(val, is(expectedVal));
      }
      index++;
    }

    // assert that total rowcount is 5500
    assertThat(index, is(5500));
  }

  /** Class to mock chunk downloader. It is just reading data from tmp directory one by one */
  private class MockChunkDownloader implements ChunkDownloader {
    private List<File> resultFileNames;

    private int currentFileIndex;

    private RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

    MockChunkDownloader(List<File> resultFileNames) {
      this.resultFileNames = resultFileNames;
      this.currentFileIndex = 0;
    }

    @Override
    public SnowflakeResultChunk getNextChunkToConsume() throws SnowflakeSQLException {
      if (currentFileIndex < resultFileNames.size()) {
        ArrowResultChunk resultChunk = new ArrowResultChunk("", 0, 0, 0, rootAllocator, null);
        try {
          InputStream is = new FileInputStream(resultFileNames.get(currentFileIndex));
          resultChunk.readArrowStream(is);

          currentFileIndex++;
          return resultChunk;
        } catch (IOException e) {
          throw new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR, "Failed " + "to read data");
        }
      } else {
        return null;
      }
    }

    @Override
    public DownloaderMetrics terminate() {
      return null;
    }
  }

  private Object[][] generateData(Schema schema, int rowCount) {
    Object[][] data = new Object[schema.getFields().size()][rowCount];

    for (int i = 0; i < schema.getFields().size(); i++) {
      Types.MinorType type = Types.getMinorTypeForArrowType(schema.getFields().get(i).getType());

      switch (type) {
        case INT:
          {
            for (int j = 0; j < rowCount; j++) {
              data[i][j] = random.nextInt();
            }
            break;
          }
        case DATEDAY:
          {
            for (int j = 0; j < rowCount; j++) {
              data[i][j] = Date.from(Instant.now());
            }
            break;
          }
        case BIGINT:
          {
            for (int j = 0; j < rowCount; j++) {
              data[i][j] = random.nextLong();
            }
            break;
          }
        case FLOAT8:
          {
            for (int j = 0; j < rowCount; j++) {
              data[i][j] = random.nextDouble();
            }
            break;
          }
        case TINYINT:
          {
            for (int j = 0; j < rowCount; j++) {
              data[i][j] = (byte) random.nextInt(1 << 8);
            }
            break;
          }
        case SMALLINT:
          {
            for (int j = 0; j < rowCount; j++) {
              data[i][j] = (short) random.nextInt(1 << 16);
            }
            break;
          }
        case VARBINARY:
          {
            for (int j = 0; j < rowCount; j++) {
              data[i][j] = RandomStringUtils.random(20).getBytes();
            }
            break;
          }
          // add other data types as needed later
      }
    }

    return data;
  }

  private File createArrowFile(
      String fileName, Schema schema, Object[][] data, int rowsPerRecordBatch) throws IOException {
    File file = resultFolder.newFile(fileName);
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    try (ArrowWriter writer =
        new ArrowStreamWriter(
            root, new DictionaryProvider.MapDictionaryProvider(), new FileOutputStream(file))) {
      writer.start();

      for (int i = 0; i < data[0].length; ) {
        int rowsToAppend = Math.min(rowsPerRecordBatch, data[0].length - i);
        root.setRowCount(rowsToAppend);

        for (int j = 0; j < data.length; j++) {
          FieldVector vector = root.getFieldVectors().get(j);

          switch (vector.getMinorType()) {
            case INT:
            case TINYINT:
            case SMALLINT:
              writeIntToField(vector, data[j], i, rowsToAppend);
              break;
            case DATEDAY:
              writeDateToField(vector, data[j], i, rowsToAppend);
              break;
            case BIGINT:
              writeLongToField(vector, data[j], i, rowsToAppend);
              break;
            case FLOAT8:
              writeDoubleToField(vector, data[j], i, rowsToAppend);
              break;
            case VARBINARY:
              writeBytesToField(vector, data[j], i, rowsToAppend);
              break;
          }
        }

        writer.writeBatch();
        i += rowsToAppend;
      }
    }

    return file;
  }

  private void writeIntToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    IntVector intVector = (IntVector) fieldVector;
    intVector.setInitialCapacity(rowsToAppend);
    intVector.allocateNew();
    for (int i = 0; i < rowsToAppend; i++) {
      intVector.setSafe(i, 1, (int) data[startIndex + i]);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeDateToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    DateDayVector datedayVector = (DateDayVector) fieldVector;
    datedayVector.setInitialCapacity(rowsToAppend);
    datedayVector.allocateNew();
    for (int i = 0; i < rowsToAppend; i++) {
      datedayVector.setSafe(i, 1, (int) data[startIndex + i]);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeLongToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    BigIntVector vector = (BigIntVector) fieldVector;
    vector.setInitialCapacity(rowsToAppend);
    vector.allocateNew();
    for (int i = 0; i < rowsToAppend; i++) {
      vector.setSafe(i, 1, (long) data[startIndex + i]);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeDoubleToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    Float8Vector vector = (Float8Vector) fieldVector;
    vector.setInitialCapacity(rowsToAppend);
    vector.allocateNew();
    for (int i = 0; i < rowsToAppend; i++) {
      vector.setSafe(i, 1, (double) data[startIndex + i]);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeBytesToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    VarBinaryVector vector = (VarBinaryVector) fieldVector;
    vector.setInitialCapacity(rowsToAppend);
    vector.allocateNew();
    for (int i = 0; i < rowsToAppend; i++) {
      vector.setSafe(i, (byte[]) data[startIndex + i], 0, ((byte[]) data[startIndex + i]).length);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }
}
