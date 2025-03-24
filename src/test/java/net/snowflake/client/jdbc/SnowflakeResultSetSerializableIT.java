package net.snowflake.client.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** SnowflakeResultSetSerializable tests */
@Tag(TestTags.RESULT_SET)
public class SnowflakeResultSetSerializableIT extends BaseJDBCTest {
  @TempDir private File tmpFolder;

  private static boolean developPrint = false;

  // sfFullURL is used to support private link URL.
  // This test case is not for private link env, so just use a valid URL for testing purpose.
  private String sfFullURL = "https://sfctest0.snowflakecomputing.com";

  public Connection init(String queryResultFormat) throws SQLException {
    return init(null, queryResultFormat);
  }

  public Connection init(@Nullable Properties properties, String queryResultFormat)
      throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(properties);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");

      // Set up theses parameters as smaller values in order to generate
      // multiple file chunks with small data volumes.
      stmt.execute("alter session set result_first_chunk_max_size = 512");
      stmt.execute("alter session set result_min_chunk_size = 512");
      stmt.execute("alter session set arrow_result_rb_flush_size = 512");
      stmt.execute("alter session set result_chunk_size_multiplier = 1.2");
    }
    return conn;
  }

  /**
   * Generate CSV string for ResultSet for correctness comparison getString() is used for each cell.
   *
   * @param rs The result set to be accessed.
   * @return The CSV string for the Result Set.
   * @throws Throwable If any error happens
   */
  private String generateCSVResult(ResultSet rs) throws Throwable {
    StringBuilder builder = new StringBuilder(1024 * 1024);
    builder.append("==== result start ===\n");

    ResultSetMetaData metadata = rs.getMetaData();
    int colCount = metadata.getColumnCount();

    while (rs.next()) {
      for (int i = 1; i <= colCount; i++) {
        rs.getObject(i);
        if (rs.wasNull()) {
          builder.append("\"").append("null").append("\",");
        } else {
          builder.append("\"").append(rs.getString(i)).append("\",");
        }
      }
      builder.append("\n");
    }

    builder.append("==== result end   ===\n");

    return builder.toString();
  }

  /**
   * Split the result set to SnowflakeResultSetSerializable objects based on the INPUT max size and
   * serialize the objects into files. One object is serialized into one separate file.
   *
   * @param rs The result set to be accessed.
   * @param maxSizeInBytes The expected data size in one serializable object.
   * @param fileNameAppendix The generated file's appendix to avoid duplicated file names
   * @return a list of file name.
   * @throws Throwable If any error happens.
   */
  private List<String> serializeResultSet(
      SnowflakeResultSet rs, long maxSizeInBytes, String fileNameAppendix) throws Throwable {
    List<String> result = new ArrayList<>();

    List<SnowflakeResultSetSerializable> resultSetChunks =
        rs.getResultSetSerializables(maxSizeInBytes);

    for (int i = 0; i < resultSetChunks.size(); i++) {
      SnowflakeResultSetSerializable entry = resultSetChunks.get(i);

      // Write object to file
      String tmpFileName = tmpFolder.getPath() + "_result_" + i + "." + fileNameAppendix;
      try (FileOutputStream fo = new FileOutputStream(tmpFileName);
          ObjectOutputStream so = new ObjectOutputStream(fo)) {
        so.writeObject(entry);
        so.flush();
      }
      result.add(tmpFileName);
    }

    if (developPrint) {
      System.out.println("\nSplit ResultSet as " + result.size() + " parts.");
      for (String filename : result) {
        System.out.println(filename);
      }
    }

    return result;
  }

  /**
   * Give a file list, deserialize SnowflakeResultSetSerializableV1 object from each file, access
   * the content with ResultSet and generate CSV string for them for correctness comparison.
   *
   * @param files The file names where the serializable objects are serialized.
   * @return The CSV string wrapped in these SnowflakeResultSetSerializableV1
   * @throws Throwable If any error happens.
   */
  private String deserializeResultSet(List<String> files) throws Throwable {
    return deserializeResultSetWithProperties(files, null);
  }

  private String deserializeResultSetWithProperties(List<String> files, Properties props)
      throws Throwable {
    StringBuilder builder = new StringBuilder(1024 * 1024);
    builder.append("==== result start ===\n");

    for (String filename : files) {
      // Read Object from file
      try (FileInputStream fi = new FileInputStream(filename);
          ObjectInputStream si = new ObjectInputStream(fi)) {
        SnowflakeResultSetSerializableV1 resultSetChunk =
            (SnowflakeResultSetSerializableV1) si.readObject();

        if (developPrint) {
          System.out.println(
              "\nFormat: "
                  + resultSetChunk.getQueryResultFormat()
                  + " UncompChunksize: "
                  + resultSetChunk.getUncompressedDataSizeInBytes()
                  + " firstChunkContent: "
                  + (resultSetChunk.getFirstChunkStringData() == null ? " null " : " not null "));
          for (SnowflakeResultSetSerializableV1.ChunkFileMetadata chunkFileMetadata :
              resultSetChunk.chunkFileMetadatas) {
            System.out.println(
                "RowCount="
                    + chunkFileMetadata.getRowCount()
                    + ", cpsize="
                    + chunkFileMetadata.getCompressedByteSize()
                    + ", uncpsize="
                    + chunkFileMetadata.getUncompressedByteSize()
                    + ", URL= "
                    + chunkFileMetadata.getFileURL());
          }
        }

        // Read data from object
        try (ResultSet rs =
            resultSetChunk.getResultSet(
                SnowflakeResultSetSerializable.ResultSetRetrieveConfig.Builder.newInstance()
                    .setProxyProperties(props)
                    .setSfFullURL(sfFullURL)
                    .build())) {

          // print result set meta data
          ResultSetMetaData metadata = rs.getMetaData();
          int colCount = metadata.getColumnCount();
          if (developPrint) {
            for (int j = 1; j <= colCount; j++) {
              System.out.print(" table: " + metadata.getTableName(j));
              System.out.print(" schema: " + metadata.getSchemaName(j));
              System.out.print(" type: " + metadata.getColumnTypeName(j));
              System.out.print(" name: " + metadata.getColumnName(j));
              System.out.print(" precision: " + metadata.getPrecision(j));
              System.out.println(" scale:" + metadata.getScale(j));
            }
          }

          // Print and count data
          while (rs.next()) {
            for (int i = 1; i <= colCount; i++) {
              rs.getObject(i);
              if (rs.wasNull()) {
                builder.append("\"").append("null").append("\",");
              } else {
                builder.append("\"").append(rs.getString(i)).append("\",");
              }
            }
            builder.append("\n");
          }
        }
      }
    }

    builder.append("==== result end   ===\n");

    return builder.toString();
  }

  /**
   * The is the test harness function for basic table.
   *
   * @param rowCount inserted row count
   * @param maxSizeInBytes expected data size in one SnowflakeResultSetSerializableV1 object.
   * @param whereClause where clause when executing query.
   * @throws Throwable If any error happens.
   */
  private void testBasicTableHarness(
      int rowCount,
      long maxSizeInBytes,
      String whereClause,
      boolean needSetupTable,
      boolean async,
      String queryResultFormat)
      throws Throwable {
    List<String> fileNameList = null;
    String originalResultCSVString = null;
    try (Connection connection = init(queryResultFormat)) {
      Statement statement = connection.createStatement();

      if (developPrint) {
        System.out.println(
            "testBasicTableHarness: rowCount="
                + rowCount
                + ", maxSizeInBytes="
                + maxSizeInBytes
                + ", whereClause="
                + whereClause
                + ", needSetupTable="
                + needSetupTable
                + ", async="
                + async);
      }

      if (needSetupTable) {
        statement.execute(
            "create or replace table table_basic " + " (int_c int, string_c string(128))");

        if (rowCount > 0) {
          statement.execute(
              "insert into table_basic select "
                  + "seq4(), 'arrow_1234567890arrow_1234567890arrow_1234567890arrow_1234567890'"
                  + " from table(generator(rowcount=>"
                  + rowCount
                  + "))");
        }
      }

      String sqlSelect = "select * from table_basic " + whereClause;
      try (ResultSet rs =
          async
              ? statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(sqlSelect)
              : statement.executeQuery(sqlSelect)) {

        fileNameList = serializeResultSet((SnowflakeResultSet) rs, maxSizeInBytes, "txt");

        originalResultCSVString = generateCSVResult(rs);
      }
    }

    String chunkResultString = deserializeResultSet(fileNameList);
    assertEquals(chunkResultString, originalResultCSVString);
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testBasicTableWithEmptyResult(String queryResultFormat) throws Throwable {
    // Use complex WHERE clause in order to test both ARROW and JSON.
    // It looks GS only generates JSON format result.
    testBasicTableHarness(10, 1024, "where int_c * int_c = 2", true, false, queryResultFormat);
    // Test Async mode
    testBasicTableHarness(10, 1024, "where int_c * int_c = 2", true, true, queryResultFormat);
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testBasicTableWithOnlyFirstChunk(String queryResultFormat) throws Throwable {
    // Result only includes first data chunk, test maxSize is small.
    testBasicTableHarness(1, 1, "", true, false, queryResultFormat);
    // Test Async mode
    testBasicTableHarness(1, 1, "", true, true, queryResultFormat);
    // Result only includes first data chunk, test maxSize is big.
    testBasicTableHarness(1, 1024 * 1024, "", false, false, queryResultFormat);
    // Test async mode
    testBasicTableHarness(1, 1024 * 1024, "", false, true, queryResultFormat);
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testBasicTableWithOneFileChunk(String queryResultFormat) throws Throwable {
    // Result only includes first data chunk, test maxSize is small.
    testBasicTableHarness(300, 1, "", true, false, queryResultFormat);
    // Test Async mode
    testBasicTableHarness(300, 1, "", true, true, queryResultFormat);
    // Result only includes first data chunk, test maxSize is big.
    testBasicTableHarness(300, 1024 * 1024, "", false, false, queryResultFormat);
    // Test Async mode
    testBasicTableHarness(300, 1024 * 1024, "", false, true, queryResultFormat);
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testBasicTableWithSomeFileChunks(String queryResultFormat) throws Throwable {
    // Result only includes first data chunk, test maxSize is small.
    testBasicTableHarness(90000, 1, "", true, false, queryResultFormat);
    // Test Async mode
    testBasicTableHarness(90000, 1, "", true, true, queryResultFormat);
    // Result only includes first data chunk, test maxSize is median.
    testBasicTableHarness(90000, 3 * 1024 * 1024, "", false, false, queryResultFormat);
    // Test Async mode
    testBasicTableHarness(90000, 3 * 1024 * 1024, "", false, true, queryResultFormat);
    // Result only includes first data chunk, test maxSize is big.
    testBasicTableHarness(90000, 100 * 1024 * 1024, "", false, false, queryResultFormat);
    // Test Async mode
    testBasicTableHarness(90000, 100 * 1024 * 1024, "", false, true, queryResultFormat);
  }

  /**
   * Test harness function to test timestamp_*, date, time types.
   *
   * @param rowCount inserted row count
   * @param maxSizeInBytes expected data size in one SnowflakeResultSetSerializableV1 object.
   * @param whereClause where clause when executing query.
   * @param format_date configuration value for DATE_OUTPUT_FORMAT
   * @param format_time configuration value for TIME_OUTPUT_FORMAT
   * @param format_ntz configuration value for TIMESTAMP_NTZ_OUTPUT_FORMAT
   * @param format_ltz configuration value for TIMESTAMP_LTZ_OUTPUT_FORMAT
   * @param format_tz configuration value for TIMESTAMP_TZ_OUTPUT_FORMAT
   * @param timezone configuration value for TIMEZONE
   * @throws Throwable If any error happens.
   */
  private void testTimestampHarness(
      int rowCount,
      long maxSizeInBytes,
      String whereClause,
      String format_date,
      String format_time,
      String format_ntz,
      String format_ltz,
      String format_tz,
      String timezone,
      String queryResultFormat)
      throws Throwable {
    List<String> fileNameList = null;
    String originalResultCSVString = null;
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {
      statement.execute("alter session set DATE_OUTPUT_FORMAT = '" + format_date + "'");
      statement.execute("alter session set TIME_OUTPUT_FORMAT = '" + format_time + "'");
      statement.execute("alter session set TIMESTAMP_NTZ_OUTPUT_FORMAT = '" + format_ntz + "'");
      statement.execute("alter session set TIMESTAMP_LTZ_OUTPUT_FORMAT = '" + format_ltz + "'");
      statement.execute("alter session set TIMESTAMP_TZ_OUTPUT_FORMAT = '" + format_tz + "'");
      statement.execute("alter session set TIMEZONE = '" + timezone + "'");

      statement.execute(
          "Create or replace table all_timestamps ("
              + "int_c int, date_c date, "
              + "time_c time, time_c0 time(0), time_c3 time(3), time_c6 time(6), "
              + "ts_ltz_c timestamp_ltz, ts_ltz_c0 timestamp_ltz(0), "
              + "ts_ltz_c3 timestamp_ltz(3), ts_ltz_c6 timestamp_ltz(6), "
              + "ts_ntz_c timestamp_ntz, ts_ntz_c0 timestamp_ntz(0), "
              + "ts_ntz_c3 timestamp_ntz(3), ts_ntz_c6 timestamp_ntz(6) "
              + ", ts_tz_c timestamp_tz, ts_tz_c0 timestamp_tz(0), "
              + "ts_tz_c3 timestamp_tz(3), ts_tz_c6 timestamp_tz(6) "
              + ")");

      if (rowCount > 0) {
        statement.execute(
            "insert into all_timestamps "
                + "select seq4(), '2015-10-25' , "
                + "'23:59:59.123456789', '23:59:59', '23:59:59.123', '23:59:59.123456', "
                + "   '2014-01-11 06:12:13.123456789', '2014-01-11 06:12:13',"
                + "   '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456',"
                + "   '2014-01-11 06:12:13.123456789', '2014-01-11 06:12:13',"
                + "   '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456',"
                + "   '2014-01-11 06:12:13.123456789', '2014-01-11 06:12:13',"
                + "   '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456'"
                + " from table(generator(rowcount=>"
                + rowCount
                + "))");
      }

      String sqlSelect = "select * from all_timestamps " + whereClause;
      try (ResultSet rs = statement.executeQuery(sqlSelect)) {

        fileNameList = serializeResultSet((SnowflakeResultSet) rs, maxSizeInBytes, "txt");

        originalResultCSVString = generateCSVResult(rs);
      }
    }

    String chunkResultString = deserializeResultSet(fileNameList);
    assertEquals(chunkResultString, originalResultCSVString);
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testTimestamp(String queryResultFormat) throws Throwable {
    String[] dateFormats = {"YYYY-MM-DD", "DD-MON-YYYY", "MM/DD/YYYY"};
    String[] timeFormats = {"HH24:MI:SS.FFTZH:TZM", "HH24:MI:SS.FF", "HH24:MI:SS"};
    String[] timestampFormats = {
      "YYYY-MM-DD HH24:MI:SS.FF3",
      "TZHTZM YYYY-MM-DD HH24:MI:SS.FF3",
      "DY, DD MON YYYY HH24:MI:SS.FF TZHTZM"
    };
    String[] timezones = {"America/Los_Angeles", "Europe/London", "GMT"};

    for (int i = 0; i < dateFormats.length; i++) {
      testTimestampHarness(
          10,
          1,
          "",
          dateFormats[i],
          timeFormats[i],
          timestampFormats[i],
          timestampFormats[i],
          timestampFormats[i],
          timezones[i],
          queryResultFormat);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testBasicTableWithSerializeObjectsAfterReadResultSet(String queryResultFormat)
      throws Throwable {
    List<String> fileNameList = null;
    String originalResultCSVString = null;
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {
      statement.execute("create or replace schema testschema");

      statement.execute(
          "create or replace table table_basic " + " (int_c int, string_c string(128))");

      int rowCount = 30000;
      statement.execute(
          "insert into table_basic select "
              + "seq4(), 'arrow_1234567890arrow_1234567890arrow_1234567890arrow_1234567890'"
              + " from table(generator(rowcount=>"
              + rowCount
              + "))");

      String sqlSelect = "select * from table_basic ";
      try (ResultSet rs = statement.executeQuery(sqlSelect)) {

        originalResultCSVString = generateCSVResult(rs);

        // In previous test, the serializable objects are serialized before
        // reading the ResultSet. This test covers the case that serializes the
        // object after reading the result set.
        fileNameList = serializeResultSet((SnowflakeResultSet) rs, 1 * 1024 * 1024, "txt");
      }
    }

    String chunkResultString = deserializeResultSet(fileNameList);
    assertEquals(chunkResultString, originalResultCSVString);
  }

  /**
   * Split the ResultSetSerializable objects based on max size.
   *
   * @param files The files where SnowflakeResultSetSerializable objects are serialized in.
   * @param maxSizeInBytes The max data size wrapped in split serializable object
   * @return a name file list where the new serializable objects resides.
   * @throws Throwable if any error occurs.
   */
  private synchronized List<String> splitResultSetSerializables(
      List<String> files, long maxSizeInBytes) throws Throwable {
    List<String> resultFileList = new ArrayList<>();

    for (String filename : files) {
      // Read Object from file
      try (FileInputStream fi = new FileInputStream(filename);
          ObjectInputStream si = new ObjectInputStream(fi)) {
        SnowflakeResultSetSerializableV1 resultSetChunk =
            (SnowflakeResultSetSerializableV1) si.readObject();

        // Get ResultSet from object
        try (ResultSet rs =
            resultSetChunk.getResultSet(
                SnowflakeResultSetSerializable.ResultSetRetrieveConfig.Builder.newInstance()
                    .setProxyProperties(new Properties())
                    .setSfFullURL(sfFullURL)
                    .build())) {

          String[] filePathParts = filename.split(File.separator);
          String appendix = filePathParts[filePathParts.length - 1];

          List<String> thisFileList =
              serializeResultSet((SnowflakeResultSet) rs, maxSizeInBytes, appendix);
          for (int i = 0; i < thisFileList.size(); i++) {
            resultFileList.add(thisFileList.get(i));
          }
        }
      }
    }

    if (developPrint) {
      System.out.println(
          "Split from " + files.size() + " files to " + resultFileList.size() + " files");
    }

    return resultFileList;
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testSplitResultSetSerializable(String queryResultFormat) throws Throwable {
    List<String> fileNameList = null;
    String originalResultCSVString = null;
    int rowCount = 90000;
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {

      statement.execute(
          "create or replace table table_basic " + " (int_c int, string_c string(128))");

      statement.execute(
          "insert into table_basic select "
              + "seq4(), "
              + "'arrow_1234567890arrow_1234567890arrow_1234567890arrow_1234567890'"
              + " from table(generator(rowcount=>"
              + rowCount
              + "))");

      String sqlSelect = "select * from table_basic ";
      try (ResultSet rs = statement.executeQuery(sqlSelect)) {

        fileNameList = serializeResultSet((SnowflakeResultSet) rs, 100 * 1024 * 1024, "txt");

        originalResultCSVString = generateCSVResult(rs);
      }
    }

    // Split deserializedResultSet by 3M, the result should be the same
    List<String> fileNameSplit3M = splitResultSetSerializables(fileNameList, 3 * 1024 * 1024);
    String chunkResultString = deserializeResultSet(fileNameSplit3M);
    assertEquals(chunkResultString, originalResultCSVString);

    // Split deserializedResultSet by 2M, the result should be the same
    List<String> fileNameSplit2M = splitResultSetSerializables(fileNameSplit3M, 2 * 1024 * 1024);
    chunkResultString = deserializeResultSet(fileNameSplit2M);
    assertEquals(chunkResultString, originalResultCSVString);

    // Split deserializedResultSet by 1M, the result should be the same
    List<String> fileNameSplit1M = splitResultSetSerializables(fileNameSplit2M, 1 * 1024 * 1024);
    chunkResultString = deserializeResultSet(fileNameSplit1M);
    assertEquals(chunkResultString, originalResultCSVString);

    // Split deserializedResultSet by smallest, the result should be the same
    List<String> fileNameSplitSmallest = splitResultSetSerializables(fileNameSplit1M, 1);
    chunkResultString = deserializeResultSet(fileNameSplitSmallest);
    assertEquals(chunkResultString, originalResultCSVString);
  }

  /**
   * Setup wrong file URL for the result set serializable objects for negative test.
   *
   * @param resultSetSerializables a list of result set serializable object.
   */
  private void hackToSetupWrongURL(List<SnowflakeResultSetSerializable> resultSetSerializables) {
    for (int i = 0; i < resultSetSerializables.size(); i++) {
      SnowflakeResultSetSerializableV1 serializableV1 =
          (SnowflakeResultSetSerializableV1) resultSetSerializables.get(i);
      for (SnowflakeResultSetSerializableV1.ChunkFileMetadata chunkFileMetadata :
          serializableV1.getChunkFileMetadatas()) {
        chunkFileMetadata.setFileURL(chunkFileMetadata.getFileURL() + "_hacked_wrong_file");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testCloseUnconsumedResultSet(String queryResultFormat) throws Throwable {
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create or replace table table_basic " + " (int_c int, string_c string(128))");

        int rowCount = 100000;
        statement.execute(
            "insert into table_basic select "
                + "seq4(), "
                + "'arrow_1234567890arrow_1234567890arrow_1234567890arrow_1234567890'"
                + " from table(generator(rowcount=>"
                + rowCount
                + "))");

        int testCount = 5;
        while (testCount-- > 0) {
          String sqlSelect = "select * from table_basic ";
          try (ResultSet rs = statement.executeQuery(sqlSelect)) {}
          ;
        }
      } finally {
        statement.execute("drop table if exists table_basic");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testNegativeWithChunkFileNotExist(String queryResultFormat) throws Throwable {
    // This test takes about (download worker retry times * networkTimeout) long to finish
    Properties properties = new Properties();
    properties.put("networkTimeout", 10000); // 10000 millisec
    try (Connection connection = init(properties, queryResultFormat)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            "create or replace table table_basic " + " (int_c int, string_c string(128))");

        int rowCount = 300;
        statement.execute(
            "insert into table_basic select "
                + "seq4(), "
                + "'arrow_1234567890arrow_1234567890arrow_1234567890arrow_1234567890'"
                + " from table(generator(rowcount=>"
                + rowCount
                + "))");

        String sqlSelect = "select * from table_basic ";
        try (ResultSet rs = statement.executeQuery(sqlSelect)) {
          // Test case 1: Generate one Serializable object
          List<SnowflakeResultSetSerializable> resultSetSerializables =
              ((SnowflakeResultSet) rs).getResultSetSerializables(100 * 1024 * 1024);

          hackToSetupWrongURL(resultSetSerializables);

          // Expected to hit credential issue when access the result.
          assertEquals(resultSetSerializables.size(), 1);
          SnowflakeResultSetSerializable resultSetSerializable = resultSetSerializables.get(0);

          ResultSet resultSet =
              resultSetSerializable.getResultSet(
                  SnowflakeResultSetSerializable.ResultSetRetrieveConfig.Builder.newInstance()
                      .setProxyProperties(new Properties())
                      .setSfFullURL(sfFullURL)
                      .build());

          SQLException ex =
              assertThrows(
                  SQLException.class,
                  () -> {
                    while (resultSet.next()) {
                      resultSet.getString(1);
                    }
                  },
                  "error should happen when accessing the data because the file URL is corrupted.");
          assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), ex.getErrorCode());
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testNegativeWithClosedResultSet(String queryResultFormat) throws Throwable {
    try (Connection connection = init(queryResultFormat)) {
      Statement statement = connection.createStatement();

      statement.execute(
          "create or replace table table_basic " + " (int_c int, string_c string(128))");

      int rowCount = 300;
      statement.execute(
          "insert into table_basic select "
              + "seq4(), "
              + "'arrow_1234567890arrow_1234567890arrow_1234567890arrow_1234567890'"
              + " from table(generator(rowcount=>"
              + rowCount
              + "))");

      String sqlSelect = "select * from table_basic ";
      ResultSet rs = statement.executeQuery(sqlSelect);
      rs.close();

      // The getResultSetSerializables() can only be called for unclosed
      // result set.
      SQLException ex =
          assertThrows(
              SQLException.class,
              () -> ((SnowflakeResultSet) rs).getResultSetSerializables(100 * 1024 * 1024));
      System.out.println("Negative test hits expected error: " + ex.getMessage());
    }
  }

  /**
   * This test is related to proxy, it can only be tested manually since there is no testing proxy
   * server setup. Below are the unit test to for the proxy. 1. Setup proxy on your testing box. The
   * instruction can be found by search "How to setup Proxy Server for Client tests" in engineering
   * pages. OR
   * https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/65438343/How+to+setup+Proxy+Server+for+Client+tests
   * 2. There are two steps to run the test manually because the proxy info cached in static
   * variables in HttpUtil. So it needs the JVM to be restarted between the 2 steps. Step 1:
   * Generate file list for SnowflakeResultSetSerializable objects. Set variable 'generateFiles' as
   * true. The file list will be printed. For example, Split ResultSet as 4 parts.
   * /tmp/junit16319222538342218700_result_0.txt /tmp/junit16319222538342218700_result_1.txt
   * /tmp/junit16319222538342218700_result_2.txt /tmp/junit16319222538342218700_result_3.txt Step 2:
   * Set variable 'generateFiles' as false. Replace the above printed file names to 'fileNameList'.
   * Run this test. The step 2 can be run with 'correctProxy' = true or false. Run it with wrong
   * proxy is to make sure the proxy setting is used.
   *
   * @throws Throwable
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @Disabled
  @DontRunOnGithubActions
  public void testCustomProxyWithFiles(String queryResultFormat) throws Throwable {
    boolean generateFiles = false;
    boolean correctProxy = false;

    if (generateFiles) {
      generateTestFiles(queryResultFormat);
      fail("This is generate test file.");
    }

    // Setup proxy information
    Properties props = new Properties();
    props.put("useProxy", "true");
    props.put("proxyHost", "localhost");
    props.put("proxyPort", "3128");
    props.put("proxyUser", "testuser1");
    if (correctProxy) {
      props.put("proxyPassword", "test");
    } else {
      props.put("proxyPassword", "wrongPasswd");
    }
    props.put("nonProxyHosts", "*.foo.com");

    // Setup files to deserialize SnowflakeResultSetSerializable objects.
    List<String> fileNameList = new ArrayList<>();
    fileNameList.add("/tmp/junit16319222538342218700_result_0.txt");
    fileNameList.add("/tmp/junit16319222538342218700_result_1.txt");
    fileNameList.add("/tmp/junit16319222538342218700_result_2.txt");
    fileNameList.add("/tmp/junit16319222538342218700_result_3.txt");

    if (correctProxy) {
      String chunkResultString = deserializeResultSetWithProperties(fileNameList, props);
      System.out.println(chunkResultString.length());
    } else {
      assertThrows(Exception.class, () -> deserializeResultSetWithProperties(fileNameList, props));
    }
  }

  private void generateTestFiles(String queryResultFormat) throws Throwable {
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {

      statement.execute(
          "create or replace table table_basic " + " (int_c int, string_c string(128))");

      int rowCount = 60000;
      statement.execute(
          "insert into table_basic select "
              + "seq4(), "
              + "'arrow_1234567890arrow_1234567890arrow_1234567890arrow_1234567890'"
              + " from table(generator(rowcount=>"
              + rowCount
              + "))");

      String sqlSelect = "select * from table_basic ";
      try (ResultSet rs = statement.executeQuery(sqlSelect)) {
        developPrint = true;
        serializeResultSet((SnowflakeResultSet) rs, 2 * 1024 * 1024, "txt");
        System.exit(-1);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testRetrieveMetadata(String queryResultFormat) throws Throwable {
    List<String> fileNameList;
    int rowCount = 90000;
    long expectedTotalRowCount = 0;
    long expectedTotalCompressedSize = 0;
    long expectedTotalUncompressedSize = 0;
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {

      statement.execute(
          "create or replace table table_basic " + " (int_c int, string_c string(128))");

      statement.execute(
          "insert into table_basic select "
              + "seq4(), "
              + "'arrow_1234567890arrow_1234567890arrow_1234567890arrow_1234567890'"
              + " from table(generator(rowcount=>"
              + rowCount
              + "))");

      String sqlSelect = "select * from table_basic ";
      try (ResultSet rs = statement.executeQuery(sqlSelect)) {
        // Split deserializedResultSet by 3M
        fileNameList = serializeResultSet((SnowflakeResultSet) rs, 100 * 1024 * 1024, "txt");

        // Only one serializable object is generated with 100M data.
        assertEquals(fileNameList.size(), 1);

        try (FileInputStream fi = new FileInputStream(fileNameList.get(0));
            ObjectInputStream si = new ObjectInputStream(fi)) {
          SnowflakeResultSetSerializableV1 wholeResultSetChunk =
              (SnowflakeResultSetSerializableV1) si.readObject();
          expectedTotalRowCount = wholeResultSetChunk.getRowCount();
          expectedTotalCompressedSize = wholeResultSetChunk.getCompressedDataSizeInBytes();
          expectedTotalUncompressedSize = wholeResultSetChunk.getUncompressedDataSizeInBytes();
        }
        if (developPrint) {
          System.out.println(
              "Total statistic: RowCount="
                  + expectedTotalRowCount
                  + " CompSize="
                  + expectedTotalCompressedSize
                  + " UncompSize="
                  + expectedTotalUncompressedSize);
        }
      }
    }
    assertEquals(expectedTotalRowCount, rowCount);
    assertThat(expectedTotalCompressedSize, greaterThan((long) 0));
    assertThat(expectedTotalUncompressedSize, greaterThan((long) 0));

    // Split deserializedResultSet by 3M
    List<String> fileNameSplit3M = splitResultSetSerializables(fileNameList, 3 * 1024 * 1024);
    // Verify the metadata is correct.
    assertTrue(
        isMetadataConsistent(
            expectedTotalRowCount,
            expectedTotalCompressedSize,
            expectedTotalUncompressedSize,
            fileNameSplit3M,
            null));

    // Split deserializedResultSet by 2M
    List<String> fileNameSplit2M = splitResultSetSerializables(fileNameSplit3M, 2 * 1024 * 1024);
    // Verify the metadata is correct.
    assertTrue(
        isMetadataConsistent(
            expectedTotalRowCount,
            expectedTotalCompressedSize,
            expectedTotalUncompressedSize,
            fileNameSplit2M,
            null));

    // Split deserializedResultSet by 3M
    List<String> fileNameSplit1M = splitResultSetSerializables(fileNameSplit2M, 1 * 1024 * 1024);
    // Verify the metadata is correct.
    assertTrue(
        isMetadataConsistent(
            expectedTotalRowCount,
            expectedTotalCompressedSize,
            expectedTotalUncompressedSize,
            fileNameSplit1M,
            null));

    // Split deserializedResultSet by smallest
    List<String> fileNameSplitSmallest = splitResultSetSerializables(fileNameSplit1M, 1);
    // Verify the metadata is correct.
    assertTrue(
        isMetadataConsistent(
            expectedTotalRowCount,
            expectedTotalCompressedSize,
            expectedTotalUncompressedSize,
            fileNameSplitSmallest,
            null));
  }

  /**
   * Give a file list, deserialize SnowflakeResultSetSerializableV1 object from each file, verify
   * the metadata on rowcount and data size to be corrected.
   *
   * @param files The file names where the serializable objects are serialized.
   * @param props additional properties for JDBC.
   * @return Return true if the metadata is consistent with the content
   * @throws Throwable If any error happens.
   */
  private boolean isMetadataConsistent(
      long expectedTotalRowCount,
      long expectedTotalCompressedSize,
      long expectedTotalUncompressedSize,
      List<String> files,
      Properties props)
      throws Throwable {
    long actualRowCountFromMetadata = 0;
    long actualTotalCompressedSize = 0;
    long actualTotalUncompressedSize = 0;
    long actualRowCount = 0;
    long chunkFileCount = 0;

    for (String filename : files) {
      // Read Object from file
      try (FileInputStream fi = new FileInputStream(filename);
          ObjectInputStream si = new ObjectInputStream(fi)) {
        SnowflakeResultSetSerializableV1 resultSetChunk =
            (SnowflakeResultSetSerializableV1) si.readObject();

        // Accumulate statistic from metadata
        actualRowCountFromMetadata += resultSetChunk.getRowCount();
        actualTotalCompressedSize += resultSetChunk.getCompressedDataSizeInBytes();
        actualTotalUncompressedSize += resultSetChunk.getUncompressedDataSizeInBytes();
        chunkFileCount += resultSetChunk.chunkFileCount;

        // Get actual row count from result set.
        // sfFullURL is used to support private link URL.
        // This test case is not for private link env, so just use a valid URL for testing purpose.
        try (ResultSet rs =
            resultSetChunk.getResultSet(
                SnowflakeResultSetSerializable.ResultSetRetrieveConfig.Builder.newInstance()
                    .setProxyProperties(props)
                    .setSfFullURL(sfFullURL)
                    .build())) {

          // Accumulate the actual row count from result set.
          while (rs.next()) {
            actualRowCount++;
          }
        }
      }
      if (developPrint) {
        System.out.println(
            "isMetadataConsistent: FileCount="
                + files.size()
                + " RowCounts="
                + expectedTotalRowCount
                + " "
                + actualRowCountFromMetadata
                + " ("
                + actualRowCount
                + ") CompSize="
                + expectedTotalCompressedSize
                + " "
                + actualTotalCompressedSize
                + " UncompSize="
                + expectedTotalUncompressedSize
                + " "
                + actualTotalUncompressedSize
                + " chunkFileCount="
                + chunkFileCount);
      }
    }
    return actualRowCount == expectedTotalRowCount
        && actualRowCountFromMetadata == expectedTotalRowCount
        && actualTotalCompressedSize == expectedTotalCompressedSize
        && expectedTotalUncompressedSize == actualTotalUncompressedSize;
  }

  @Test
  public void testResultSetRetrieveConfig() throws Throwable {
    SnowflakeResultSetSerializable.ResultSetRetrieveConfig.Builder builder =
        SnowflakeResultSetSerializable.ResultSetRetrieveConfig.Builder.newInstance();

    boolean hitExpectedException = false;
    try {
      builder.setSfFullURL("sfctest0.snowflakecomputing.com");
      // The URL is invalid because it doesn't include protocol, it should raise exception
      builder.build();
    } catch (Exception ex) {
      hitExpectedException = true;
    }
    assertTrue(hitExpectedException);
  }
}
