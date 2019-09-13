package net.snowflake.client.jdbc;


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * SnowflakeResultSetSerializable tests
 */
@RunWith(Parameterized.class)
public class SnowflakeResultSetSerializableIT extends BaseJDBCTest
{
  @Parameterized.Parameters(name = "format={0}")
  public static Object[][] data()
  {
    // all tests in this class need to run for both query result formats json
    // and arrow
    if (BaseJDBCTest.isArrowTestsEnabled())
    {
      return new Object[][]{
          {"JSON"}
          , {"Arrow_force"}
      };
    }
    else
    {
      return new Object[][]{
          {"JSON"}
      };
    }
  }

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static boolean developPrint = true;

  private static String queryResultFormat;

  public SnowflakeResultSetSerializableIT(String format)
  {
    queryResultFormat = format;
  }

  public static Connection getConnection()
      throws SQLException
  {
    Connection conn = BaseJDBCTest.getConnection();
    if (isArrowTestsEnabled())
    {
      conn.createStatement().execute(
          "alter session set query_result_format = '" + queryResultFormat + "'");
    }

    // Set up theses parameters as smaller values in order to generate
    // multiple file chunks with small data volumes.
    conn.createStatement().execute(
        "alter session set result_first_chunk_max_size = 512");
    conn.createStatement().execute(
        "alter session set result_min_chunk_size = 512");
    conn.createStatement().execute(
        "alter session set arrow_result_rb_flush_size = 512");
    conn.createStatement().execute(
        "alter session set result_chunk_size_multiplier = 1.2");

    return conn;
  }

  /**
   * Generate CSV string for ResultSet for correctness comparison
   * getString() is used for each cell.
   *
   * @param rs The result set to be accessed.
   * @return The CSV string for the Result Set.
   * @throws Throwable If any error happens
   */
  private String generateCSVResult(ResultSet rs) throws Throwable
  {
    StringBuilder builder = new StringBuilder(1024 * 1024);
    builder.append("==== result start ===\n");

    ResultSetMetaData metadata = rs.getMetaData();
    int colCount = metadata.getColumnCount();

    while (rs.next())
    {
      for (int i = 1; i <= colCount; i++)
      {
        builder.append("\"").append(rs.getString(i)).append("\",");
      }
      builder.append("\n");
    }

    builder.append("==== result end   ===\n");

    return builder.toString();
  }

  /**
   * Split the result set to SnowflakeResultSetSerializable objects based on
   * the INPUT max size and serialize the objects into files. One object is
   * serialized into one separate file.
   *
   * @param rs The result set to be accessed.
   * @param maxSizeInBytes The expected data size in one serializable object.
   * @return a list of file name.
   * @throws Throwable If any error happens.
   */
  private List<String> serializeResultSet(SnowflakeResultSet rs,
                                          long maxSizeInBytes)
      throws Throwable
  {
    List<String> result = new ArrayList<>();

    List<SnowflakeResultSetSerializable> resultSetChunks =
        rs.getResultSetSerializables(maxSizeInBytes);

    for (int i = 0; i < resultSetChunks.size(); i++)
    {
      SnowflakeResultSetSerializable entry = resultSetChunks.get(i);

      // Write object to file
      String tmpFileName =
          tmpFolder.getRoot().getPath() + "_result_" + i + ".txt";
      FileOutputStream fo = new FileOutputStream(tmpFileName);
      ObjectOutputStream so = new ObjectOutputStream(fo);
      so.writeObject(entry);
      so.flush();
      so.close();

      result.add(tmpFileName);
    }

    if (developPrint)
    {
      System.out.println("\nSplit ResultSet as " + result.size() + " parts.");
    }

    return result;
  }

  /**
   * Give a file list, deserialize SnowflakeResultSetSerializableV1 object from
   * each file, access the content with ResultSet and generate CSV string for
   * them for correctness comparison.
   *
   * @param files The file names where the serializable objects are serialized.
   * @return The CSV string wrapped in these SnowflakeResultSetSerializableV1
   * @throws Throwable If any error happens.
   */
  private String deserializeResultSet(List<String> files) throws Throwable
  {
    StringBuilder builder = new StringBuilder(1024 * 1024);
    builder.append("==== result start ===\n");

    for (String filename : files)
    {
      // Read Object from file
      FileInputStream fi = new FileInputStream(filename);
      ObjectInputStream si = new ObjectInputStream(fi);
      SnowflakeResultSetSerializableV1 resultSetChunk =
          (SnowflakeResultSetSerializableV1) si.readObject();
      fi.close();

      if (developPrint)
      {
        System.out.println(
            "\nFormat: " + resultSetChunk.getQueryResultFormat() +
                " UncompChunksize: " +
                resultSetChunk.getUncompressedDataSize() +
                " firstChunkContent: " +
                (resultSetChunk.getFirstChunkStringData() == null
                    ? " null " : " not null "));
        for (SnowflakeResultSetSerializableV1.ChunkFileMetadata chunkFileMetadata :
            resultSetChunk.chunkFileMetadatas)
        {
          System.out.println(
              "RowCount=" + chunkFileMetadata.getRowCount()
                  + ", cpsize=" + chunkFileMetadata.getCompressedByteSize()
                  + ", uncpsize=" +
                  chunkFileMetadata.getUncompressedByteSize()
                  + ", URL= " + chunkFileMetadata.getFileURL());
        }
      }

      // Read data from object
      ResultSet rs = resultSetChunk.getResultSet();

      // print result set meta data
      ResultSetMetaData metadata = rs.getMetaData();
      int colCount = metadata.getColumnCount();
      if (developPrint)
      {
        for (int j = 1; j <= colCount; j++)
        {
          System.out.print(" table: " + metadata.getTableName(j));
          System.out.print(" schema: " + metadata.getSchemaName(j));
          System.out.print(" type: " + metadata.getColumnTypeName(j));
          System.out.print(" name: " + metadata.getColumnName(j));
          System.out.print(" precision: " + metadata.getPrecision(j));
          System.out.println(" scale:" + metadata.getScale(j));
        }
      }

      // Print and count data
      while (rs.next())
      {
        for (int i = 1; i <= colCount; i++)
        {
          builder.append("\"").append(rs.getString(i)).append("\",");
        }
        builder.append("\n");
      }
    }

    builder.append("==== result end   ===\n");

    return builder.toString();
  }

  /**
   * The is the test harness function for basic table.
   *
   *
   * @param rowCount inserted row count
   * @param maxSizeInBytes expected data size in one
   *                       SnowflakeResultSetSerializableV1 object.
   * @param whereClause where clause when executing query.
   * @throws Throwable If any error happens.
   */
  private void testBasicTableHarness(int rowCount, long maxSizeInBytes,
                                     String whereClause,
                                     boolean needSetupTable) throws Throwable
  {
    List<String> fileNameList = null;
    String originalResultCSVString = null;
    try (Connection connection = getConnection())
    {
      Statement statement = connection.createStatement();

      if (needSetupTable)
      {
        statement.execute(
            "create or replace table table_basic " +
                " (int_c int, string_c string(128))");

        if (rowCount > 0)
        {
          statement.execute(
              "insert into table_basic select " +
                  "seq4(), 'arrow_1234567890arrow_1234567890arrow_1234567890arrow_1234567890'" +
                  " from table(generator(rowcount=>" + rowCount + "))");
        }
      }

      String sqlSelect = "select * from table_basic " + whereClause;
      ResultSet rs = statement.executeQuery(sqlSelect);

      fileNameList = serializeResultSet((SnowflakeResultSet) rs,
                                           maxSizeInBytes);

      originalResultCSVString = generateCSVResult(rs);
      rs.close();
    }

    String chunkResultString = deserializeResultSet(fileNameList);
    assertTrue(chunkResultString.equals(originalResultCSVString));
  }

  @Test
  public void testBasicTable_EmptyResult()  throws Throwable
  {
    // Use complex WHERE clause in order to test both ARROW and JSON.
    // It looks GS only generates JSON format result.
    testBasicTableHarness(10, 1024, "where int_c * int_c = 2", true);
  }

  @Test
  public void testBasicTable_OnlyFirstChunk()  throws Throwable
  {
    // Result only includes first data chunk, test maxSize is small.
    testBasicTableHarness(1, 1, "", true);
    // Result only includes first data chunk, test maxSize is big.
    testBasicTableHarness(1, 1024*1024, "", false);
  }

  @Test
  public void testBasicTable_OneFileChunk()  throws Throwable
  {
    // Result only includes first data chunk, test maxSize is small.
    testBasicTableHarness(300, 1, "", true);
    // Result only includes first data chunk, test maxSize is big.
    testBasicTableHarness(300, 1024*1024, "", false);
  }

  @Test
  public void testBasicTable_SomeFileChunks()  throws Throwable
  {
    // Result only includes first data chunk, test maxSize is small.
    testBasicTableHarness(90000, 1, "", true);
    // Result only includes first data chunk, test maxSize is median.
    testBasicTableHarness(90000, 3*1024*1024, "",false);
    // Result only includes first data chunk, test maxSize is big.
    testBasicTableHarness(90000, 100*1024*1024, "",false);
  }

  /**
   * Test harness function to test timestamp_*, date, time types.
   *
   * @param rowCount inserted row count
   * @param maxSizeInBytes expected data size in one
   *                       SnowflakeResultSetSerializableV1 object.
   * @param whereClause where clause when executing query.
   * @param format_date configuration value for DATE_OUTPUT_FORMAT
   * @param format_time configuration value for TIME_OUTPUT_FORMAT
   * @param format_ntz configuration value for TIMESTAMP_NTZ_OUTPUT_FORMAT
   * @param format_ltz configuration value for TIMESTAMP_LTZ_OUTPUT_FORMAT
   * @param format_tz configuration value for TIMESTAMP_TZ_OUTPUT_FORMAT
   * @param timezone configuration value for TIMEZONE
   * @throws Throwable If any error happens.
   */
  private void testTimestampHarness(int rowCount,
                                    long maxSizeInBytes,
                                    String whereClause,
                                    String format_date,
                                    String format_time,
                                    String format_ntz,
                                    String format_ltz,
                                    String format_tz,
                                    String timezone)
      throws Throwable
  {
    List<String> fileNameList = null;
    String originalResultCSVString = null;
    try (Connection connection = getConnection())
    {
      connection.createStatement().execute(
          "alter session set DATE_OUTPUT_FORMAT = '" + format_date + "'");
      connection.createStatement().execute(
          "alter session set TIME_OUTPUT_FORMAT = '" + format_time + "'");
      connection.createStatement().execute(
          "alter session set TIMESTAMP_NTZ_OUTPUT_FORMAT = '" + format_ntz + "'");
      connection.createStatement().execute(
          "alter session set TIMESTAMP_LTZ_OUTPUT_FORMAT = '" + format_ltz + "'");
      connection.createStatement().execute(
          "alter session set TIMESTAMP_TZ_OUTPUT_FORMAT = '" + format_tz + "'");
      connection.createStatement().execute(
          "alter session set TIMEZONE = '" + timezone + "'");

      Statement statement = connection.createStatement();

      statement.execute(
          "Create or replace table all_timestamps (" +
              "int_c int, date_c date, " +
              "time_c time, time_c0 time(0), time_c3 time(3), time_c6 time(6), " +
              "ts_ltz_c timestamp_ltz, ts_ltz_c0 timestamp_ltz(0), " +
              "ts_ltz_c3 timestamp_ltz(3), ts_ltz_c6 timestamp_ltz(6), " +
              "ts_ntz_c timestamp_ntz, ts_ntz_c0 timestamp_ntz(0), " +
              "ts_ntz_c3 timestamp_ntz(3), ts_ntz_c6 timestamp_ntz(6) " +
              ", ts_tz_c timestamp_tz, ts_tz_c0 timestamp_tz(0), " +
              "ts_tz_c3 timestamp_tz(3), ts_tz_c6 timestamp_tz(6) " +
              ")");

      if (rowCount > 0)
      {
        connection.createStatement().execute(
            "insert into all_timestamps " +
                "select seq4(), '2015-10-25' , " +
                "'23:59:59.123456789', '23:59:59', '23:59:59.123', '23:59:59.123456', " +
                "   '2014-01-11 06:12:13.123456789', '2014-01-11 06:12:13'," +
                "   '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456'," +

                "   '2014-01-11 06:12:13.123456789', '2014-01-11 06:12:13'," +
                "   '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456'," +

                "   '2014-01-11 06:12:13.123456789', '2014-01-11 06:12:13'," +
                "   '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456'" +
                " from table(generator(rowcount=>" + rowCount + "))");
      }

      String sqlSelect = "select * from all_timestamps " + whereClause;
      ResultSet rs = statement.executeQuery(sqlSelect);

      fileNameList = serializeResultSet((SnowflakeResultSet) rs,
                                        maxSizeInBytes);

      originalResultCSVString = generateCSVResult(rs);
      rs.close();
    }

    String chunkResultString = deserializeResultSet(fileNameList);
    assertTrue(chunkResultString.equals(originalResultCSVString));
  }

  @Test
  public void testTimestamp()  throws Throwable
  {
    String[] dateFormats = {"YYYY-MM-DD", "DD-MON-YYYY", "MM/DD/YYYY"};
    String[] timeFormats = {"HH24:MI:SS.FFTZH:TZM",
                            "HH24:MI:SS.FF",
                            "HH24:MI:SS"};
    String[] timestampFormats = {"YYYY-MM-DD HH24:MI:SS.FF3",
                                 "TZHTZM YYYY-MM-DD HH24:MI:SS.FF3",
                                 "DY, DD MON YYYY HH24:MI:SS.FF TZHTZM"};
    String[] timezongs = {"America/Los_Angeles", "Europe/London", "GMT"};

    for (int i = 0; i < dateFormats.length; i++)
    {
      testTimestampHarness(10, 1, "",
                          dateFormats[i], timeFormats[i], timestampFormats[i],
                          timestampFormats[i], timestampFormats[i], timezongs[i]);
    }
  }
}
