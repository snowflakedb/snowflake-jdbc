package net.snowflake.client.core;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import net.snowflake.client.annotations.DontRunOnThinJar;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.ArrowResultChunk;
import net.snowflake.client.jdbc.BaseJDBCWithSharedConnectionIT;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeResultChunk;
import net.snowflake.client.jdbc.SnowflakeResultSet;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializable;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.telemetry.NoOpTelemetryClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(TestTags.ARROW)
public class SFArrowResultSetIT extends BaseJDBCWithSharedConnectionIT {
  private Random random = new Random();

  /**
   * allocator for arrow RootAllocator is shaded so it cannot be overridden when testing thin or fat
   * jar
   */
  protected BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  /** temporary folder to store result files */
  @TempDir private File tempDir;

  /** Test the case that all results are returned in first chunk */
  @Test
  @DontRunOnThinJar
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

    try (InputStream is = new FileInputStream(file)) {
      is.read(dataBytes, 0, dataSize);
    }

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
  @DontRunOnThinJar
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
  @DontRunOnThinJar
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

    try (InputStream is = new FileInputStream(arrowFile)) {
      is.read(dataBytes, 0, dataSize);
    }

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
        try (InputStream is = new FileInputStream(resultFileNames.get(currentFileIndex))) {
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

  Object[][] generateData(Schema schema, int rowCount) {
    Object[][] data = new Object[schema.getFields().size()][rowCount];

    for (int i = 0; i < schema.getFields().size(); i++) {
      Types.MinorType type = Types.getMinorTypeForArrowType(schema.getFields().get(i).getType());

      switch (type) {
        case BIT:
          {
            for (int j = 0; j < rowCount; j++) {
              data[i][j] = random.nextBoolean();
            }
            break;
          }
        case INT:
          {
            for (int j = 0; j < rowCount; j++) {
              data[i][j] = 0;
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
        case DECIMAL:
          {
            for (int j = 0; j < rowCount; j++) {
              data[i][j] = 154639183700000l;
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
        case VARCHAR:
          {
            for (int j = 0; j < rowCount; j++) {
              data[i][j] = RandomStringUtils.random(20);
            }
            break;
          }
          // add other data types as needed later
      }
    }

    return data;
  }

  File createArrowFile(String fileName, Schema schema, Object[][] data, int rowsPerRecordBatch)
      throws IOException {
    File file = new File(tempDir, fileName);
    file.createNewFile();
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    try (FileOutputStream fos = new FileOutputStream(file);
        ArrowWriter writer =
            new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider(), fos)) {
      writer.start();

      for (int i = 0; i < data[0].length; ) {
        int rowsToAppend = Math.min(rowsPerRecordBatch, data[0].length - i);
        root.setRowCount(rowsToAppend);

        for (int j = 0; j < data.length; j++) {
          FieldVector vector = root.getFieldVectors().get(j);

          switch (vector.getMinorType()) {
            case BIT:
              writeBitToField(vector, data[j], i, rowsToAppend);
              break;
            case INT:
              writeIntToField(vector, data[j], i, rowsToAppend);
              break;
            case TINYINT:
              writeTinyIntToField(vector, data[j], i, rowsToAppend);
              break;
            case SMALLINT:
              writeSmallIntToField(vector, data[j], i, rowsToAppend);
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
            case VARCHAR:
              writeTextToField(vector, data[j], i, rowsToAppend);
              break;
            case DECIMAL:
              writeDecimalToField(vector, data[j], i, rowsToAppend);
              break;
            case STRUCT:
              writeTimestampStructToField(vector, data[j], data[j + 1], i, rowsToAppend);
              j++;
              break;
          }
        }

        writer.writeBatch();
        i += rowsToAppend;
      }
    }

    return file;
  }

  private void writeLongToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    BigIntVector vector = (BigIntVector) fieldVector;

    vector.setInitialCapacity(rowsToAppend);
    vector.allocateNew();
    vector.setNull(0);
    for (int i = 0; i < rowsToAppend; i++) {
      vector.setSafe(i, 1, (long) data[startIndex + i]);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeBitToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    BitVector vector = (BitVector) fieldVector;
    vector.setInitialCapacity(rowsToAppend);
    vector.allocateNew();
    vector.setNull(0);
    for (int i = 1; i < rowsToAppend; i++) {
      int val = (Boolean) data[startIndex + i] == true ? 1 : 0;
      vector.setSafe(i, 1, val);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeDateToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    DateDayVector datedayVector = (DateDayVector) fieldVector;
    datedayVector.setInitialCapacity(rowsToAppend);
    datedayVector.allocateNew();
    datedayVector.setNull(0);
    for (int i = 1; i < rowsToAppend; i++) {
      datedayVector.setSafe(i, 1, (int) ((Date) data[startIndex + i]).getTime() / 1000);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeDecimalToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    DecimalVector datedayVector = (DecimalVector) fieldVector;
    datedayVector.setInitialCapacity(rowsToAppend);
    datedayVector.allocateNew();
    datedayVector.setNull(0);
    for (int i = 1; i < rowsToAppend; i++) {
      datedayVector.setSafe(i, (long) data[startIndex + i]);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeDoubleToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    Float8Vector vector = (Float8Vector) fieldVector;
    vector.setInitialCapacity(rowsToAppend);
    vector.allocateNew();
    vector.setNull(0);
    for (int i = 1; i < rowsToAppend; i++) {
      vector.setSafe(i, 1, (double) data[startIndex + i]);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeIntToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    IntVector intVector = (IntVector) fieldVector;
    intVector.setInitialCapacity(rowsToAppend);
    intVector.allocateNew();
    intVector.setNull(0);
    for (int i = 1; i < rowsToAppend; i++) {
      intVector.setSafe(i, 1, (int) data[startIndex + i]);
    }
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeSmallIntToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    SmallIntVector intVector = (SmallIntVector) fieldVector;
    intVector.setInitialCapacity(rowsToAppend);
    intVector.allocateNew();
    intVector.setNull(0);
    for (int i = 1; i < rowsToAppend; i++) {
      intVector.setSafe(i, 1, (short) data[startIndex + i]);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeTinyIntToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    TinyIntVector vector = (TinyIntVector) fieldVector;
    vector.setInitialCapacity(rowsToAppend);
    vector.allocateNew();
    vector.setNull(0);
    for (int i = 1; i < rowsToAppend; i++) {
      vector.setSafe(i, 1, (byte) data[startIndex + i]);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeBytesToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    VarBinaryVector vector = (VarBinaryVector) fieldVector;
    vector.setInitialCapacity(rowsToAppend);
    vector.allocateNew();
    vector.setNull(0);
    for (int i = 1; i < rowsToAppend; i++) {
      vector.setSafe(i, (byte[]) data[startIndex + i], 0, ((byte[]) data[startIndex + i]).length);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeTextToField(
      FieldVector fieldVector, Object[] data, int startIndex, int rowsToAppend) {
    VarCharVector intVector = (VarCharVector) fieldVector;
    intVector.setInitialCapacity(rowsToAppend);
    intVector.allocateNew();
    intVector.setNull(0);
    for (int i = 1; i < rowsToAppend; i++) {
      intVector.setSafe(i, new Text((String) data[startIndex + i]));
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  private void writeTimestampStructToField(
      FieldVector fieldVector, Object[] data, Object[] data2, int startIndex, int rowsToAppend) {
    StructVector vector = (StructVector) fieldVector;
    vector.setInitialCapacity(rowsToAppend);
    vector.allocateNew();
    vector.setNull(0);
    for (int i = 1; i < rowsToAppend; i++) {
      List<FieldVector> childVectors = vector.getChildrenFromFields();
      BigIntVector v1 = (BigIntVector) childVectors.get(0);
      v1.setSafe(i, 1, (long) data[startIndex + i]);

      IntVector v2 = (IntVector) childVectors.get(1);
      v2.setSafe(i, 1, (int) data2[startIndex + i]);
    }
    // how many are set
    fieldVector.setValueCount(rowsToAppend);
  }

  /** Test that first chunk containing struct vectors (used for timestamps) can be sorted */
  @Test
  @DontRunOnThinJar
  public void testSortedResultChunkWithStructVectors() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      statement.execute("create or replace table teststructtimestamp (t1 timestamp_ltz)");
      try (ResultSet rs = statement.executeQuery("select * from teststructtimestamp")) {
        List<SnowflakeResultSetSerializable> resultSetSerializables =
            ((SnowflakeResultSet) rs).getResultSetSerializables(100 * 1024 * 1024);
        SnowflakeResultSetSerializableV1 resultSetSerializable =
            (SnowflakeResultSetSerializableV1) resultSetSerializables.get(0);

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

        try (InputStream is = new FileInputStream(file)) {
          is.read(dataBytes, 0, dataSize);
        }

        resultSetSerializable.setRootAllocator(new RootAllocator(Long.MAX_VALUE));
        resultSetSerializable.setFirstChunkStringData(
            Base64.getEncoder().encodeToString(dataBytes));
        resultSetSerializable.setFirstChunkByteData(dataBytes);
        resultSetSerializable.setChunkFileCount(0);

        SFArrowResultSet resultSet =
            new SFArrowResultSet(resultSetSerializable, new NoOpTelemetryClient(), true);

        for (int i = 0; i < 1000; i++) {
          resultSet.next();
        }
        // We inserted a null row at the beginning so when sorted, the last row should be null
        assertEquals(null, resultSet.getObject(1));
        assertFalse(resultSet.next());
        statement.execute("drop table teststructtimestamp;");
      }
    }
  }

  /** Test that the first chunk can be sorted */
  @Test
  @DontRunOnThinJar
  public void testSortedResultChunk() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace table alltypes (i1 int, d1 date, b1 bigint, f1 float, s1 smallint, t1 tinyint, b2 binary, t2 text, b3 boolean, d2 decimal)");
      try (ResultSet rs = statement.executeQuery("select * from alltypes")) {
        List<SnowflakeResultSetSerializable> resultSetSerializables =
            ((SnowflakeResultSet) rs).getResultSetSerializables(100 * 1024 * 1024);
        SnowflakeResultSetSerializableV1 resultSetSerializable =
            (SnowflakeResultSetSerializableV1) resultSetSerializables.get(0);

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
        File file = createArrowFile("testVectorTypes", schema, data, 10);

        int dataSize = (int) file.length();
        byte[] dataBytes = new byte[dataSize];

        try (InputStream is = new FileInputStream(file)) {
          is.read(dataBytes, 0, dataSize);
        }

        resultSetSerializable.setRootAllocator(new RootAllocator(Long.MAX_VALUE));
        resultSetSerializable.setFirstChunkStringData(
            Base64.getEncoder().encodeToString(dataBytes));
        resultSetSerializable.setFirstChunkByteData(dataBytes);
        resultSetSerializable.setChunkFileCount(0);

        SFArrowResultSet resultSet =
            new SFArrowResultSet(resultSetSerializable, new NoOpTelemetryClient(), true);

        for (int i = 0; i < 1000; i++) {
          resultSet.next();
        }
        // We inserted a null row at the beginning so when sorted, the last row should be null
        assertEquals(null, resultSet.getObject(1));
        assertFalse(resultSet.next());
        statement.execute("drop table alltypes;");
      }
    }
  }
}
