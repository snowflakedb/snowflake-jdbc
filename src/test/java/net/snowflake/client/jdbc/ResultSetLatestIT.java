package net.snowflake.client.jdbc;

import static net.snowflake.client.TestUtil.expectSnowflakeLoggedFeatureNotSupportedException;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import net.snowflake.client.TestUtil;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SessionUtil;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import net.snowflake.client.jdbc.telemetry.TelemetryField;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import net.snowflake.common.core.SFBinary;
import org.apache.arrow.vector.Float8Vector;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * ResultSet integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the oldest supported driver to examine
 * if the tests still is not applicable. If it is applicable, move tests to ResultSetIT so that both
 * the latest and oldest supported driver run the tests.
 */
@Tag(TestTags.RESULT_SET)
public class ResultSetLatestIT extends ResultSet0IT {
  private static void setQueryResultFormat(Statement stmt, String queryResultFormat)
      throws SQLException {
    stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
  }

  private String createTableSql =
      "Create or replace table get_object_for_numeric_types (c1 INT, c2 BIGINT, c3 SMALLINT, c4 TINYINT) ";
  private String insertStmt =
      "Insert into get_object_for_numeric_types (c1, c2, c3, c4) values (1000000000, 2000000000000000000000000, 3, 4)";
  private String selectQuery = "Select * from get_object_for_numeric_types";
  private String setJdbcTreatDecimalAsIntFalse =
      "alter session set JDBC_TREAT_DECIMAL_AS_INT = false";

  /**
   * Test that when closing of results is interrupted by Thread.Interrupt(), the memory is released
   * safely before driver execution ends.
   *
   * @throws Throwable
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testMemoryClearingAfterInterrupt(String queryResultFormat) throws Throwable {
    try (Statement statement = createStatement(queryResultFormat)) {
      final long initialMemoryUsage = SnowflakeChunkDownloader.getCurrentMemoryUsage();
      // Inject an InterruptedException into the SnowflakeChunkDownloader.terminate() function
      SnowflakeChunkDownloader.setInjectedDownloaderException(new InterruptedException());
      // 10000 rows should be enough to force result into multiple chunks

      SQLException ex =
          assertThrows(
              SQLException.class,
              () -> {
                try (ResultSet resultSet =
                    statement.executeQuery(
                        "select seq8(), randstr(1000, random()) from table(generator(rowcount => 10000))")) {
                  assertThat(
                      "hold memory usage for the resultSet before close",
                      SnowflakeChunkDownloader.getCurrentMemoryUsage() - initialMemoryUsage >= 0);
                  // Result closure should catch InterruptedException and throw a SQLException after
                  // its caught
                }
              });
      assertEquals((int) ErrorCode.INTERRUPTED.getMessageCode(), ex.getErrorCode());
      // Assert all memory was released
      assertThat(
          "closing statement didn't release memory allocated for result",
          SnowflakeChunkDownloader.getCurrentMemoryUsage(),
          equalTo(initialMemoryUsage));
      // Unset the exception injection so statement and connection can close without exceptions
      SnowflakeChunkDownloader.setInjectedDownloaderException(null);
    }
  }

  /**
   * This tests that the SnowflakeChunkDownloader doesn't hang when memory limits are low. Opening
   * multiple statements concurrently uses a lot of memory. This checks that chunks download even
   * when there is not enough memory available for concurrent prefetching.
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testChunkDownloaderNoHang(String queryResultFormat) throws SQLException {
    int stmtCount = 30;
    int rowCount = 170000;
    try (Statement stmt = createStatement(queryResultFormat)) {
      List<ResultSet> rsList = new ArrayList<>();
      // Set memory limit to low number
      connection
          .unwrap(SnowflakeConnectionV1.class)
          .getSFBaseSession()
          .setMemoryLimitForTesting(2000000);
      // open multiple statements concurrently to overwhelm current memory allocation
      for (int i = 0; i < stmtCount; ++i) {
        ResultSet resultSet =
            stmt.executeQuery(
                "select randstr(100, random()) from table(generator(rowcount => "
                    + rowCount
                    + "))");
        rsList.add(resultSet);
      }
      // Assert that all resultSets exist and can successfully download the needed chunks without
      // hanging
      for (int i = 0; i < stmtCount; i++) {
        rsList.get(i).next();
        assertTrue(Pattern.matches("[a-zA-Z0-9]{100}", rsList.get(i).getString(1)));
        rsList.get(i).close();
      }
      // set memory limit back to default invalid value so it does not get used
      connection
          .unwrap(SnowflakeConnectionV1.class)
          .getSFBaseSession()
          .setMemoryLimitForTesting(SFBaseSession.MEMORY_LIMIT_UNSET);
    }
  }

  /** This tests that the SnowflakeChunkDownloader doesn't hang when memory limits are low. */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testChunkDownloaderSetRetry(String queryResultFormat) throws SQLException {
    int stmtCount = 3;
    int rowCount = 170000;
    try (Statement stmt = createStatement(queryResultFormat)) {
      connection
          .unwrap(SnowflakeConnectionV1.class)
          .getSFBaseSession()
          .setMemoryLimitForTesting(1 * 1024 * 1024);
      connection
          .unwrap(SnowflakeConnectionV1.class)
          .getSFBaseSession()
          .setOtherParameter(SessionUtil.JDBC_CHUNK_DOWNLOADER_MAX_RETRY, 1);
      // Set memory limit to low number
      // open multiple statements concurrently to overwhelm current memory allocation
      for (int i = 0; i < stmtCount; ++i) {
        try (ResultSet resultSet =
            stmt.executeQuery(
                "select randstr(100, random()) from table(generator(rowcount => "
                    + rowCount
                    + "))")) {
          // consume half of the results and go to the next statement
          for (int j = 0; j < rowCount / 2; j++) {
            resultSet.next();
          }
          assertTrue(Pattern.matches("[a-zA-Z0-9]{100}", resultSet.getString(1)));
        }
      }
      // reset retry to MAX_NUM_OF_RETRY, which is 10
      connection
          .unwrap(SnowflakeConnectionV1.class)
          .getSFBaseSession()
          .setOtherParameter(SessionUtil.JDBC_CHUNK_DOWNLOADER_MAX_RETRY, 10);
      // set memory limit back to default invalid value so it does not get used
      connection
          .unwrap(SnowflakeConnectionV1.class)
          .getSFBaseSession()
          .setMemoryLimitForTesting(SFBaseSession.MEMORY_LIMIT_UNSET);
    }
  }

  /**
   * Metadata API metric collection. The old driver didn't collect metrics.
   *
   * @throws SQLException arises if any exception occurs
   * @throws ExecutionException arises if error occurred when sending telemetry events
   * @throws InterruptedException arises if error occurred when sending telemetry events
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testMetadataAPIMetricCollection(String queryResultFormat)
      throws SQLException, ExecutionException, InterruptedException {
    Statement stmt = createStatement(queryResultFormat);
    stmt.close();
    Telemetry telemetry =
        connection.unwrap(SnowflakeConnectionV1.class).getSfSession().getTelemetryClient();
    DatabaseMetaData metadata = connection.getMetaData();
    // Call one of the DatabaseMetadata API functions but for simplicity, ensure returned
    // ResultSet
    // is empty
    metadata.getColumns("fakecatalog", "fakeschema", null, null);
    LinkedList<TelemetryData> logs = ((TelemetryClient) telemetry).logBuffer();
    // No result set has been downloaded from server so no chunk downloader metrics have been
    // collected
    // Logs should contain 1 item: the data about the getColumns() parameters
    assertEquals(logs.size(), 1);
    // Assert the log is of type client_metadata_api_metrics
    assertEquals(
        logs.get(0).getMessage().get(TelemetryUtil.TYPE).textValue(),
        TelemetryField.METADATA_METRICS.toString());
    // Assert function name and params match and that query id exists
    assertEquals(logs.get(0).getMessage().get("function_name").textValue(), "getColumns");
    TestUtil.assertValidQueryId(logs.get(0).getMessage().get("query_id").textValue());
    JsonNode parameterValues = logs.get(0).getMessage().get("function_parameters");
    assertEquals(parameterValues.get("catalog").textValue(), "fakecatalog");
    assertEquals(parameterValues.get("schema").textValue(), "fakeschema");
    assertNull(parameterValues.get("general_name_pattern").textValue());
    assertNull(parameterValues.get("specific_name_pattern").textValue());

    // send data to clear log for next test
    telemetry.sendBatchAsync().get();
    assertEquals(0, ((TelemetryClient) telemetry).logBuffer().size());

    String catalog = connection.getCatalog();
    String schema = connection.getSchema();
    metadata.getColumns(catalog, schema, null, null);
    logs = ((TelemetryClient) telemetry).logBuffer();
    assertEquals(logs.size(), 2);
    // first item in log buffer is metrics on time to consume first result set chunk
    assertEquals(
        logs.get(0).getMessage().get(TelemetryUtil.TYPE).textValue(),
        TelemetryField.TIME_CONSUME_FIRST_RESULT.toString());
    // second item in log buffer is metrics on getProcedureColumns() parameters
    // Assert the log is of type client_metadata_api_metrics
    assertEquals(
        logs.get(1).getMessage().get(TelemetryUtil.TYPE).textValue(),
        TelemetryField.METADATA_METRICS.toString());
    // Assert function name and params match and that query id exists
    assertEquals(logs.get(1).getMessage().get("function_name").textValue(), "getColumns");
    TestUtil.assertValidQueryId(logs.get(1).getMessage().get("query_id").textValue());
    parameterValues = logs.get(1).getMessage().get("function_parameters");
    assertEquals(parameterValues.get("catalog").textValue(), catalog);
    assertEquals(parameterValues.get("schema").textValue(), schema);
    assertNull(parameterValues.get("general_name_pattern").textValue());
    assertNull(parameterValues.get("specific_name_pattern").textValue());
  }

  /**
   * Test that there is no nullptr exception thrown when null value is retrieved from character
   * stream
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetCharacterStreamNull(String queryResultFormat) throws SQLException {
    try (Statement statement = createStatement(queryResultFormat)) {
      statement.execute("create or replace table JDBC_NULL_CHARSTREAM (col1 varchar(16))");
      statement.execute("insert into JDBC_NULL_CHARSTREAM values(NULL)");
      try (ResultSet rs = statement.executeQuery("select * from JDBC_NULL_CHARSTREAM")) {
        rs.next();
        assertNull(rs.getCharacterStream(1));
      }
    }
  }

  /**
   * Large result chunk metrics tests. The old driver didn't collect metrics.
   *
   * @throws SQLException arises if any exception occurs
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testMultipleChunks(String queryResultFormat) throws Exception {
    try (Statement statement = createStatement(queryResultFormat);

        // 10000 rows should be enough to force result into multiple chunks
        ResultSet resultSet =
            statement.executeQuery(
                "select seq8(), randstr(1000, random()) from table(generator(rowcount => 10000))")) {
      int cnt = 0;
      while (resultSet.next()) {
        ++cnt;
      }
      assertTrue(cnt >= 0);
      Telemetry telemetry =
          connection.unwrap(SnowflakeConnectionV1.class).getSfSession().getTelemetryClient();
      LinkedList<TelemetryData> logs = ((TelemetryClient) telemetry).logBuffer();

      // there should be a log for each of the following fields
      TelemetryField[] expectedFields = {
        TelemetryField.TIME_CONSUME_FIRST_RESULT, TelemetryField.TIME_CONSUME_LAST_RESULT,
        TelemetryField.TIME_WAITING_FOR_CHUNKS, TelemetryField.TIME_DOWNLOADING_CHUNKS,
        TelemetryField.TIME_PARSING_CHUNKS
      };
      boolean[] succeeded = new boolean[expectedFields.length];

      for (int i = 0; i < expectedFields.length; i++) {
        succeeded[i] = false;
        for (TelemetryData log : logs) {
          if (log.getMessage()
              .get(TelemetryUtil.TYPE)
              .textValue()
              .equals(expectedFields[i].field)) {
            succeeded[i] = true;
            break;
          }
        }
      }

      for (int i = 0; i < expectedFields.length; i++) {
        assertThat(
            String.format("%s field not found in telemetry logs\n", expectedFields[i].field),
            succeeded[i]);
      }
      telemetry.sendBatchAsync();
    }
  }

  /**
   * Result set metadata
   *
   * @throws SQLException arises if any exception occurs
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testResultSetMetadata(String queryResultFormat) throws SQLException {
    final Map<String, String> params = getConnectionParameters();
    try (Statement statement = createStatement(queryResultFormat)) {
      try {
        statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
        statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
        ResultSet resultSet = statement.executeQuery("select * from test_rsmd");
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        assertEquals(
            params.get("database").toUpperCase(),
            resultSetMetaData.getCatalogName(1).toUpperCase());
        assertEquals(
            params.get("schema").toUpperCase(), resultSetMetaData.getSchemaName(1).toUpperCase());
        assertEquals("TEST_RSMD", resultSetMetaData.getTableName(1));
        assertEquals(String.class.getName(), resultSetMetaData.getColumnClassName(2));
        assertEquals(2, resultSetMetaData.getColumnCount());
        assertEquals(22, resultSetMetaData.getColumnDisplaySize(1));
        assertEquals("COLA", resultSetMetaData.getColumnLabel(1));
        assertEquals("COLA", resultSetMetaData.getColumnName(1));
        assertEquals(3, resultSetMetaData.getColumnType(1));
        assertEquals("NUMBER", resultSetMetaData.getColumnTypeName(1));
        assertEquals(20, resultSetMetaData.getPrecision(1));
        assertEquals(5, resultSetMetaData.getScale(1));
        assertFalse(resultSetMetaData.isAutoIncrement(1));
        assertFalse(resultSetMetaData.isCaseSensitive(1));
        assertFalse(resultSetMetaData.isCurrency(1));
        assertFalse(resultSetMetaData.isDefinitelyWritable(1));
        assertEquals(ResultSetMetaData.columnNullable, resultSetMetaData.isNullable(1));
        assertTrue(resultSetMetaData.isReadOnly(1));
        assertTrue(resultSetMetaData.isSearchable(1));
        assertTrue(resultSetMetaData.isSigned(1));
        SnowflakeResultSetMetaData secretMetaData =
            resultSetMetaData.unwrap(SnowflakeResultSetMetaData.class);
        List<String> colNames = secretMetaData.getColumnNames();
        assertEquals("COLA", colNames.get(0));
        assertEquals("COLB", colNames.get(1));
        assertEquals(Types.DECIMAL, secretMetaData.getInternalColumnType(1));
        assertEquals(Types.VARCHAR, secretMetaData.getInternalColumnType(2));
        TestUtil.assertValidQueryId(secretMetaData.getQueryID());
      } finally {
        statement.execute("drop table if exists test_rsmd");
      }
    }
  }

  /**
   * Tests behavior of empty ResultSet. Only callable from getGeneratedKeys().
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testEmptyResultSet(String queryResultFormat) throws SQLException {
    try (Statement statement = createStatement(queryResultFormat);
        // the only function that returns ResultSetV1.emptyResultSet()
        ResultSet rs = statement.getGeneratedKeys()) {
      assertFalse(rs.next());
      assertFalse(rs.isClosed());
      assertEquals(0, rs.getInt(1));
      assertEquals(0, rs.getInt("col1"));
      assertEquals(0L, rs.getLong(2));
      assertEquals(0L, rs.getLong("col2"));
      assertEquals(0, rs.getShort(3));
      assertEquals(0, rs.getShort("col3"));
      assertEquals("", rs.getString(4));
      assertEquals("", rs.getString("col4"));
      assertEquals(0, rs.getDouble(5), 0);
      assertEquals(0, rs.getDouble("col5"), 0);
      assertEquals(0, rs.getFloat(6), 0);
      assertEquals(0, rs.getFloat("col6"), 0);
      assertEquals(false, rs.getBoolean(7));
      assertEquals(false, rs.getBoolean("col7"));
      assertEquals((byte) 0, rs.getByte(8));
      assertEquals((byte) 0, rs.getByte("col8"));
      assertEquals(null, rs.getBinaryStream(9));
      assertEquals(null, rs.getBinaryStream("col9"));
      assertEquals(null, rs.getDate(10));
      assertEquals(null, rs.getDate(10, new FakeCalendar()));
      assertEquals(null, rs.getDate("col10"));
      assertEquals(null, rs.getDate("col10", new FakeCalendar()));
      assertEquals(null, rs.getTime(11));
      assertEquals(null, rs.getTime(11, new FakeCalendar()));
      assertEquals(null, rs.getTime("col11"));
      assertEquals(null, rs.getTime("col11", new FakeCalendar()));
      assertEquals(null, rs.getTimestamp(12));
      assertEquals(null, rs.getTimestamp(12, new FakeCalendar()));
      assertEquals(null, rs.getTimestamp("col12"));
      assertEquals(null, rs.getTimestamp("col12", new FakeCalendar()));
      assertEquals(null, rs.getDate(13));
      assertEquals(null, rs.getDate("col13"));
      assertEquals(null, rs.getAsciiStream(14));
      assertEquals(null, rs.getAsciiStream("col14"));
      assertArrayEquals(new byte[0], rs.getBytes(15));
      assertArrayEquals(new byte[0], rs.getBytes("col15"));
      assertNull(rs.getBigDecimal(16));
      assertNull(rs.getBigDecimal(16, 38));
      assertNull(rs.getBigDecimal("col16"));
      assertNull(rs.getBigDecimal("col16", 38));
      assertNull(rs.getRef(17));
      assertNull(rs.getRef("col17"));
      assertNull(rs.getArray(18));
      assertNull(rs.getArray("col18"));
      assertNull(rs.getBlob(19));
      assertNull(rs.getBlob("col19"));
      assertNull(rs.getClob(20));
      assertNull(rs.getClob("col20"));
      assertEquals(0, rs.findColumn("col1"));
      assertNull(rs.getUnicodeStream(21));
      assertNull(rs.getUnicodeStream("col21"));
      assertNull(rs.getURL(22));
      assertNull(rs.getURL("col22"));
      assertNull(rs.getObject(23));
      assertNull(rs.getObject("col24"));
      assertNull(rs.getObject(23, SnowflakeResultSetV1.class));
      assertNull(rs.getObject("col23", SnowflakeResultSetV1.class));
      assertNull(rs.getNString(25));
      assertNull(rs.getNString("col25"));
      assertNull(rs.getNClob(26));
      assertNull(rs.getNClob("col26"));
      assertNull(rs.getNCharacterStream(27));
      assertNull(rs.getNCharacterStream("col27"));
      assertNull(rs.getCharacterStream(28));
      assertNull(rs.getCharacterStream("col28"));
      assertNull(rs.getSQLXML(29));
      assertNull(rs.getSQLXML("col29"));
      assertNull(rs.getStatement());
      assertNull(rs.getWarnings());
      assertNull(rs.getCursorName());
      assertNull(rs.getMetaData());
      assertNull(rs.getRowId(1));
      assertNull(rs.getRowId("col1"));
      assertEquals(0, rs.getRow());
      assertEquals(0, rs.getFetchDirection());
      assertEquals(0, rs.getFetchSize());
      assertEquals(0, rs.getType());
      assertEquals(0, rs.getConcurrency());
      assertEquals(0, rs.getHoldability());
      assertNull(rs.unwrap(SnowflakeResultSetV1.class));
      assertFalse(rs.isWrapperFor(SnowflakeResultSetV1.class));
      assertFalse(rs.wasNull());
      assertFalse(rs.isFirst());
      assertFalse(rs.isBeforeFirst());
      assertFalse(rs.isLast());
      assertFalse(rs.isAfterLast());
      assertFalse(rs.first());
      assertFalse(rs.last());
      assertFalse(rs.previous());
      assertFalse(rs.rowUpdated());
      assertFalse(rs.rowInserted());
      assertFalse(rs.rowDeleted());
      assertFalse(rs.absolute(1));
      assertFalse(rs.relative(1));
    }
  }

  /**
   * Gets bytes from other data types
   *
   * @throws Exception arises if any exception occurs.
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testBytesCrossTypeTests(String queryResultFormat) throws Exception {
    try (ResultSet resultSet = numberCrossTesting(queryResultFormat)) {
      assertTrue(resultSet.next());
      // assert that 0 is returned for null values for every type of value
      for (int i = 1; i < 13; i++) {
        assertArrayEquals(null, resultSet.getBytes(i));
      }
      assertTrue(resultSet.next());
      assertArrayEquals(intToByteArray(2), resultSet.getBytes(1));
      assertArrayEquals(intToByteArray(5), resultSet.getBytes(2));
      assertArrayEquals(floatToByteArray(3.5f), resultSet.getBytes(3));
      assertArrayEquals(new byte[] {1}, resultSet.getBytes(4));
      assertArrayEquals(new byte[] {(byte) '1'}, resultSet.getBytes(5));
      assertArrayEquals("1".getBytes(), resultSet.getBytes(6));

      for (int i = 7; i < 12; i++) {
        int finalI = i;
        SQLException ex =
            assertThrows(SQLException.class, () -> resultSet.getBytes(finalI), "Failing on " + i);
        assertEquals(200038, ex.getErrorCode());
      }

      byte[] decoded = SFBinary.fromHex("48454C4C4F").getBytes();

      assertArrayEquals(decoded, resultSet.getBytes(12));
    }
  }

  // SNOW-204185
  // 30s for timeout. This test usually finishes in around 10s.
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @Timeout(30)
  public void testResultChunkDownloaderException(String queryResultFormat) throws SQLException {
    try (Statement statement = createStatement(queryResultFormat)) {

      // The generated resultSet must be big enough for triggering result chunk downloader
      String query =
          "select current_date(), true,2345234, 2343.0, 'testrgint\\n"
              + "\\t' from table(generator(rowcount=>10000))";

      try (ResultSet resultSet = statement.executeQuery(query)) {
        assertTrue(resultSet.next()); // should finish successfully
      }

      try {
        SnowflakeChunkDownloader.setInjectedDownloaderException(
            new OutOfMemoryError("Fake OOM error for testing"));
        try (ResultSet resultSet = statement.executeQuery(query)) {
          // Normally this step won't cause too long. Because we will get exception once trying to
          // get
          // result from the first chunk downloader
          assertThrows(
              SnowflakeSQLException.class,
              () -> {
                while (resultSet.next()) {}
              },
              "Should not reach here. Last next() command is supposed to throw an exception");
        }
      } finally {
        SnowflakeChunkDownloader.setInjectedDownloaderException(null);
      }
    }
  }

  /**
   * SNOW-165204 Fix exception that resulted from fetching too large a number with getObject().
   *
   * @throws SQLException
   */
  @Test
  public void testGetObjectWithBigInt() throws SQLException {
    try (Statement statement = createStatement("json")) {
      // test with greatest possible number and greatest negative possible number
      String[] extremeNumbers = {
        "99999999999999999999999999999999999999", "-99999999999999999999999999999999999999"
      };
      for (int i = 0; i < extremeNumbers.length; i++) {
        try (ResultSet resultSet = statement.executeQuery("select " + extremeNumbers[i])) {
          assertTrue(resultSet.next());
          assertEquals(Types.BIGINT, resultSet.getMetaData().getColumnType(1));
          assertEquals(new BigDecimal(extremeNumbers[i]), resultSet.getObject(1));
        }
      }
    }
  }

  private byte[] intToByteArray(int i) {
    return BigInteger.valueOf(i).toByteArray();
  }

  private byte[] floatToByteArray(float i) {
    return ByteBuffer.allocate(Float8Vector.TYPE_WIDTH).putDouble(0, i).array();
  }

  /**
   * Test getBigDecimal(int colIndex, int scale) works properly and doesn't throw any
   * NullPointerExceptions (SNOW-334161)
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetBigDecimalWithScale(String queryResultFormat) throws SQLException {
    try (Statement statement = createStatement(queryResultFormat)) {
      statement.execute("create or replace table test_get(colA number(38,9))");
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("insert into test_get values(?)")) {
        preparedStatement.setBigDecimal(1, null);
        preparedStatement.addBatch();
        BigDecimal bigDecimal = new BigDecimal("100000000.123456789");
        preparedStatement.setBigDecimal(1, bigDecimal);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();

        try (ResultSet resultSet = statement.executeQuery("select * from test_get")) {
          assertTrue(resultSet.next());
          assertEquals(null, resultSet.getBigDecimal(1, 5));
          assertEquals(null, resultSet.getBigDecimal("COLA", 5));
          assertTrue(resultSet.next());
          assertEquals(bigDecimal.setScale(5, RoundingMode.HALF_UP), resultSet.getBigDecimal(1, 5));
          assertEquals(
              bigDecimal.setScale(5, RoundingMode.HALF_UP), resultSet.getBigDecimal("COLA", 5));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetDataTypeWithTimestampTz(String queryResultFormat) throws Exception {
    try (Connection connection = getConnection()) {
      ResultSetMetaData resultSetMetaData = null;
      try (Statement statement = connection.createStatement()) {
        setQueryResultFormat(statement, queryResultFormat);
        statement.executeQuery("create or replace table ts_test(ts timestamp_tz)");
        try (ResultSet resultSet = statement.executeQuery("select * from ts_test")) {
          resultSetMetaData = resultSet.getMetaData();
          // Assert that TIMESTAMP_TZ type matches java.sql.TIMESTAMP_WITH_TIMEZONE
          assertEquals(resultSetMetaData.getColumnType(1), 2014);
          // Assert that TIMESTAMP_TZ column returns Timestamp class name
          assertEquals(resultSetMetaData.getColumnClassName(1), Timestamp.class.getName());
        }
      }
      SFBaseSession baseSession = connection.unwrap(SnowflakeConnectionV1.class).getSFBaseSession();
      Field field = SFBaseSession.class.getDeclaredField("enableReturnTimestampWithTimeZone");
      field.setAccessible(true);
      field.set(baseSession, false);

      try (Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery("select * from ts_test")) {
        resultSetMetaData = resultSet.getMetaData();
        // Assert that TIMESTAMP_TZ type matches java.sql.TIMESTAMP when
        // enableReturnTimestampWithTimeZone is false.
        assertEquals(resultSetMetaData.getColumnType(1), Types.TIMESTAMP);
      }
    }
  }

  /**
   * Test getClob(int), and getClob(String) handle SQL nulls and don't throw a NullPointerException
   * (SNOW-749517)
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetEmptyOrNullClob(String queryResultFormat) throws SQLException {
    Clob clob = connection.createClob();
    clob.setString(1, "hello world");
    Clob emptyClob = connection.createClob();
    emptyClob.setString(1, "");
    try (Statement statement = createStatement(queryResultFormat)) {
      statement.execute(
          "create or replace table test_get_clob(colA varchar, colNull varchar, colEmpty text)");
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("insert into test_get_clob values(?, ?, ?)")) {
        preparedStatement.setClob(1, clob);
        preparedStatement.setString(2, null);
        preparedStatement.setClob(3, emptyClob);
        preparedStatement.execute();
      }
      try (ResultSet resultSet = statement.executeQuery("select * from test_get_clob")) {
        assertTrue(resultSet.next());
        assertEquals("hello world", resultSet.getClob(1).toString());
        assertEquals("hello world", resultSet.getClob("COLA").toString());
        assertNull(resultSet.getClob(2));
        assertNull(resultSet.getClob("COLNULL"));
        assertEquals("", resultSet.getClob(3).toString());
        assertEquals("", resultSet.getClob("COLEMPTY").toString());
      }
    }
  }

  /**
   * Since now getClob(x) can return a null, theoretically someone may work with a null Clob and try
   * to use the setClob(int, Clob) method which will result in a NullPointerException. (SNOW-749517)
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSetNullClob(String queryResultFormat) throws SQLException {
    Clob clob = null;
    try (Statement statement = createStatement(queryResultFormat)) {
      statement.execute("create or replace table test_set_clob(colNull varchar)");
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("insert into test_set_clob values(?)")) {
        preparedStatement.setClob(1, clob);
        preparedStatement.execute();
      }

      try (ResultSet resultSet = statement.executeQuery("select * from test_set_clob")) {
        assertTrue(resultSet.next());
        assertNull(resultSet.getClob(1));
        assertNull(resultSet.getClob("COLNULL"));
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testCallStatementType(String queryResultFormat) throws SQLException {
    Properties props = new Properties();
    props.put("USE_STATEMENT_TYPE_CALL_FOR_STORED_PROC_CALLS", "true");
    try (Connection connection = getConnection(props);
        Statement statement = connection.createStatement()) {
      setQueryResultFormat(statement, queryResultFormat);
      try {
        String sp =
            "CREATE OR REPLACE PROCEDURE \"SP_ZSDLEADTIME_ARCHIVE_DAILY\"()\n"
                + "RETURNS VARCHAR\n"
                + "LANGUAGE SQL\n"
                + "EXECUTE AS CALLER\n"
                + "AS \n"
                + "'\n"
                + "declare\n"
                + "result varchar;\n"
                + " \n"
                + "    begin\n"
                + "        BEGIN TRANSACTION;\n"
                + "      \n"
                + "        --Delete records older than 1 year\n"
                + "        DELETE FROM MYTABLE1 WHERE ID < 5;\n"
                + "       \n"
                + "        --Insert new records\n"
                + "        INSERT INTO MYTABLE1\n"
                + "            (ID,\n"
                + "            NAME\n"
                + "            )\n"
                + "            SELECT   \n"
                + "            SEQ,FIRST_NAME\n"
                + "            FROM MYCSVTABLE;\n"
                + "        \n"
                + "COMMIT;\n"
                + "result := ''SUCCESS'';\n"
                + "return result;\n"
                + "exception\n"
                + "    when other then\n"
                + "        begin\n"
                + "        ROLLBACK;\n"
                + "            --Insert record about error\n"
                + "            let line := ''sp-sql-msg: '' || SQLERRM || '' code : '' || SQLCODE;\n"
                + "\n"
                + "            let sp_name := ''SP_ZSDLEADTIME_ARCHIVE_DAILY'';\n"
                + "            INSERT into MYTABLE1 values (1000, :line);\n"
                + "        raise;\n"
                + "    end;\n"
                + "end;\n"
                + "';";
        statement.execute("create or replace table MYCSVTABLE (SEQ int, FIRST_NAME string)");
        statement.execute("create or replace table MYTABLE1 (ID int, NAME string)");
        statement.execute(sp);

        try (CallableStatement cs = connection.prepareCall("CALL SP_ZSDLEADTIME_ARCHIVE_DAILY()")) {
          cs.execute();
          ResultSetMetaData resultSetMetaData = cs.getMetaData();
          assertEquals("SP_ZSDLEADTIME_ARCHIVE_DAILY", resultSetMetaData.getColumnName(1));
          assertEquals("VARCHAR", resultSetMetaData.getColumnTypeName(1));
          assertEquals(0, resultSetMetaData.getScale(1));
        }
      } finally {
        statement.execute("drop procedure if exists SP_ZSDLEADTIME_ARCHIVE_DAILY()");
        statement.execute("drop table if exists MYTABLE1");
        statement.execute("drop table if exists MYCSVTABLE");
      }
    }
  }

  /**
   * Test that new query error message function for checking async query error messages is not
   * implemented for synchronous queries *
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testNewFeaturesNotSupportedExeceptions(String queryResultFormat) throws SQLException {
    try (Statement statement = createStatement(queryResultFormat);
        ResultSet rs = statement.executeQuery("select 1")) {
      expectSnowflakeLoggedFeatureNotSupportedException(
          rs.unwrap(SnowflakeResultSet.class)::getQueryErrorMessage);
      expectSnowflakeLoggedFeatureNotSupportedException(
          rs.unwrap(SnowflakeResultSet.class)::getStatus);
      expectSnowflakeLoggedFeatureNotSupportedException(() -> rs.getArray(1));
      expectSnowflakeLoggedFeatureNotSupportedException(
          () -> rs.unwrap(SnowflakeBaseResultSet.class).getList(1, String.class));
      expectSnowflakeLoggedFeatureNotSupportedException(
          () -> rs.unwrap(SnowflakeBaseResultSet.class).getArray(1, String.class));
      expectSnowflakeLoggedFeatureNotSupportedException(
          () -> rs.unwrap(SnowflakeBaseResultSet.class).getMap(1, String.class));

      expectSnowflakeLoggedFeatureNotSupportedException(
          () -> rs.unwrap(SnowflakeBaseResultSet.class).getUnicodeStream(1));
      expectSnowflakeLoggedFeatureNotSupportedException(
          () -> rs.unwrap(SnowflakeBaseResultSet.class).getUnicodeStream("column1"));
      expectSnowflakeLoggedFeatureNotSupportedException(
          () ->
              rs.unwrap(SnowflakeBaseResultSet.class)
                  .updateAsciiStream("column1", new FakeInputStream(), 5L));
      expectSnowflakeLoggedFeatureNotSupportedException(
          () ->
              rs.unwrap(SnowflakeBaseResultSet.class)
                  .updateBinaryStream("column1", new FakeInputStream(), 5L));
      expectSnowflakeLoggedFeatureNotSupportedException(
          () ->
              rs.unwrap(SnowflakeBaseResultSet.class)
                  .updateCharacterStream("column1", new FakeReader(), 5L));

      expectSnowflakeLoggedFeatureNotSupportedException(
          () ->
              rs.unwrap(SnowflakeBaseResultSet.class)
                  .updateAsciiStream(1, new FakeInputStream(), 5L));
      expectSnowflakeLoggedFeatureNotSupportedException(
          () ->
              rs.unwrap(SnowflakeBaseResultSet.class)
                  .updateBinaryStream(1, new FakeInputStream(), 5L));
      expectSnowflakeLoggedFeatureNotSupportedException(
          () ->
              rs.unwrap(SnowflakeBaseResultSet.class)
                  .updateCharacterStream(1, new FakeReader(), 5L));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testInvalidUnWrap(String queryResultFormat) throws SQLException {
    try (ResultSet rs = createStatement(queryResultFormat).executeQuery("select 1")) {
      SQLException ex = assertThrows(SQLException.class, () -> rs.unwrap(SnowflakeUtil.class));
      assertEquals(
          ex.getMessage(),
          "net.snowflake.client.jdbc.SnowflakeResultSetV1 not unwrappable from net.snowflake.client.jdbc.SnowflakeUtil");
    }
  }

  @Test
  public void testGetObjectJsonResult() throws SQLException {
    try (Statement statement = createStatement("json")) {
      try {
        statement.execute("create or replace table testObj (colA double, colB boolean)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into testObj values(?, ?)")) {
          preparedStatement.setDouble(1, 22.2);
          preparedStatement.setBoolean(2, true);
          preparedStatement.executeQuery();
        }
        try (ResultSet resultSet = statement.executeQuery("select * from testObj")) {
          assertTrue(resultSet.next());
          assertEquals(22.2, resultSet.getObject(1));
          assertEquals(true, resultSet.getObject(2));
        }
      } finally {
        statement.execute("drop table if exists testObj");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testMetadataIsCaseSensitive(String queryResultFormat) throws SQLException {
    try (Statement statement = createStatement(queryResultFormat)) {

      String sampleCreateTableWithAllColTypes =
          "CREATE or replace TABLE case_sensitive ("
              + "  boolean_col BOOLEAN,"
              + "  date_col DATE,"
              + "  time_col TIME,"
              + "  timestamp_col TIMESTAMP,"
              + "  timestamp_ltz_col TIMESTAMP_LTZ,"
              + "  timestamp_ntz_col TIMESTAMP_NTZ,"
              + "  number_col NUMBER,"
              + "  float_col FLOAT,"
              + "  double_col DOUBLE,"
              + "  binary_col BINARY,"
              + "  geography_col GEOGRAPHY,"
              + "  variant_col VARIANT,"
              + "  object_col1 OBJECT,"
              + "  array_col1 ARRAY,"
              + "  text_col1 TEXT,"
              + "  varchar_col VARCHAR(16777216),"
              + "  char_col CHAR(16777216)"
              + ");";

      statement.execute(sampleCreateTableWithAllColTypes);
      try (ResultSet rs = statement.executeQuery("select * from case_sensitive")) {
        ResultSetMetaData metaData = rs.getMetaData();

        assertFalse(metaData.isCaseSensitive(1)); // BOOLEAN
        assertFalse(metaData.isCaseSensitive(2)); // DATE
        assertFalse(metaData.isCaseSensitive(3)); // TIME
        assertFalse(metaData.isCaseSensitive(4)); // TIMESTAMP
        assertFalse(metaData.isCaseSensitive(5)); // TIMESTAMP_LTZ
        assertFalse(metaData.isCaseSensitive(6)); // TIMESTAMP_NTZ
        assertFalse(metaData.isCaseSensitive(7)); // NUMBER
        assertFalse(metaData.isCaseSensitive(8)); // FLOAT
        assertFalse(metaData.isCaseSensitive(9)); // DOUBLE
        assertFalse(metaData.isCaseSensitive(10)); // BINARY

        assertTrue(metaData.isCaseSensitive(11)); // GEOGRAPHY
        assertTrue(metaData.isCaseSensitive(12)); // VARIANT
        assertTrue(metaData.isCaseSensitive(13)); // OBJECT
        assertTrue(metaData.isCaseSensitive(14)); // ARRAY
        assertTrue(metaData.isCaseSensitive(15)); // TEXT
        assertTrue(metaData.isCaseSensitive(16)); // VARCHAR
        assertTrue(metaData.isCaseSensitive(17)); // CHAR
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testAutoIncrementResult(String queryResultFormat) throws SQLException {
    Properties paramProperties = new Properties();
    paramProperties.put("ENABLE_FIX_759900", true);
    try (Connection connection = init(paramProperties, queryResultFormat);
        Statement statement = connection.createStatement()) {

      statement.execute(
          "create or replace table auto_inc(id int autoincrement, name varchar(10), another_col int autoincrement)");
      statement.execute("insert into auto_inc(name) values('test1')");

      try (ResultSet resultSet = statement.executeQuery("select * from auto_inc")) {
        assertTrue(resultSet.next());

        ResultSetMetaData metaData = resultSet.getMetaData();
        assertTrue(metaData.isAutoIncrement(1));
        assertFalse(metaData.isAutoIncrement(2));
        assertTrue(metaData.isAutoIncrement(3));
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGranularTimeFunctionsInSessionTimezone(String queryResultFormat)
      throws SQLException {
    try (Statement statement = createStatement(queryResultFormat)) {
      try {
        statement.execute("create or replace table testGranularTime(t time)");
        statement.execute("insert into testGranularTime values ('10:10:10')");
        try (ResultSet resultSet = statement.executeQuery("select * from testGranularTime")) {
          assertTrue(resultSet.next());
          assertEquals(Time.valueOf("10:10:10"), resultSet.getTime(1));
          assertEquals(10, resultSet.getTime(1).getHours());
          assertEquals(10, resultSet.getTime(1).getMinutes());
          assertEquals(10, resultSet.getTime(1).getSeconds());
        }
      } finally {
        statement.execute("drop table if exists testGranularTime");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testGranularTimeFunctionsInUTC(String queryResultFormat) throws SQLException {
    TimeZone origTz = TimeZone.getDefault();
    try (Statement statement = createStatement(queryResultFormat)) {
      try {
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
        statement.execute("alter session set JDBC_USE_SESSION_TIMEZONE=false");
        statement.execute("create or replace table testGranularTime(t time)");
        statement.execute("insert into testGranularTime values ('10:10:10')");
        try (ResultSet resultSet = statement.executeQuery("select * from testGranularTime")) {
          assertTrue(resultSet.next());
          assertEquals(Time.valueOf("02:10:10"), resultSet.getTime(1));
          assertEquals(02, resultSet.getTime(1).getHours());
          assertEquals(10, resultSet.getTime(1).getMinutes());
          assertEquals(10, resultSet.getTime(1).getSeconds());
        }
      } finally {
        TimeZone.setDefault(origTz);
        statement.execute("drop table if exists testGranularTime");
      }
    }
  }

  /** Added in > 3.14.5 */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testLargeStringRetrieval(String queryResultFormat) throws SQLException {
    String originalMaxJsonStringLength =
        System.getProperty(ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM);
    System.clearProperty(ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM);
    String tableName = "maxJsonStringLength_table";
    int colLength = 16777216;
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      setQueryResultFormat(statement, queryResultFormat);
      SFBaseSession session = con.unwrap(SnowflakeConnectionV1.class).getSFBaseSession();
      Integer maxVarcharSize =
          (Integer) session.getOtherParameter("VARCHAR_AND_BINARY_MAX_SIZE_IN_RESULT");
      if (maxVarcharSize != null) {
        colLength = maxVarcharSize;
      }
      statement.execute("create or replace table " + tableName + " (c1 string(" + colLength + "))");
      statement.execute(
          "insert into " + tableName + " select randstr(" + colLength + ", random())");
      try (ResultSet rs = statement.executeQuery("select * from " + tableName)) {
        assertTrue(rs.next());
        assertEquals(colLength, rs.getString(1).length());
        assertFalse(rs.next());
      }
    } finally {
      if (originalMaxJsonStringLength != null) {
        System.setProperty(
            ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM, originalMaxJsonStringLength);
      }
    }
  }

  private static void assertAllColumnsAreLongButBigIntIsBigDecimal(ResultSet rs)
      throws SQLException {
    while (rs.next()) {
      assertEquals(Long.class, rs.getObject(1).getClass());
      assertEquals(BigDecimal.class, rs.getObject(2).getClass());
      assertEquals(Long.class, rs.getObject(3).getClass());
      assertEquals(Long.class, rs.getObject(4).getClass());
    }
  }

  private static void assertAllColumnsAreBigDecimal(ResultSet rs) throws SQLException {
    while (rs.next()) {
      assertEquals(BigDecimal.class, rs.getObject(1).getClass());
      assertEquals(BigDecimal.class, rs.getObject(2).getClass());
      assertEquals(BigDecimal.class, rs.getObject(3).getClass());
      assertEquals(BigDecimal.class, rs.getObject(4).getClass());
    }
  }

  // Test setting new connection property JDBC_ARROW_TREAT_DECIMAL_AS_INT=false. Connection property
  // introduced after version 3.15.0.
  @Test
  public void testGetObjectForArrowResultFormatJDBCArrowDecimalAsIntFalse() throws SQLException {
    Properties properties = new Properties();
    properties.put("JDBC_ARROW_TREAT_DECIMAL_AS_INT", false);
    try (Connection con = getConnection(properties);
        Statement stmt = con.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = 'ARROW'");
      stmt.execute(createTableSql);
      stmt.execute(insertStmt);

      // Test with JDBC_ARROW_TREAT_DECIMAL_AS_INT=false and JDBC_TREAT_DECIMAL_AS_INT=true
      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        assertAllColumnsAreLongButBigIntIsBigDecimal(rs);
      }

      // Test with JDBC_ARROW_TREAT_DECIMAL_AS_INT=false and JDBC_TREAT_DECIMAL_AS_INT=false
      stmt.execute(setJdbcTreatDecimalAsIntFalse);
      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        assertAllColumnsAreBigDecimal(rs);
      }
    }
  }

  // Test default setting of new connection property JDBC_ARROW_TREAT_DECIMAL_AS_INT=true.
  // Connection property introduced after version 3.15.0.
  @Test
  public void testGetObjectForArrowResultFormatJDBCArrowDecimalAsIntTrue() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = 'ARROW'");
      stmt.execute(createTableSql);
      stmt.execute(insertStmt);

      // Test with JDBC_ARROW_TREAT_DECIMAL_AS_INT=true and JDBC_TREAT_DECIMAL_AS_INT=true
      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        assertAllColumnsAreLongButBigIntIsBigDecimal(rs);
      }

      // Test with JDBC_ARROW_TREAT_DECIMAL_AS_INT=true and JDBC_TREAT_DECIMAL_AS_INT=false
      stmt.execute(setJdbcTreatDecimalAsIntFalse);
      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        assertAllColumnsAreLongButBigIntIsBigDecimal(rs);
      }
    }
  }

  // Test getObject for numeric types when JDBC_TREAT_DECIMAL_AS_INT is set and using JSON result
  // format.
  @Test
  public void testGetObjectForJSONResultFormatUsingJDBCDecimalAsInt() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = 'JSON'");
      stmt.execute(createTableSql);
      stmt.execute(insertStmt);

      // Test with JDBC_TREAT_DECIMAL_AS_INT=true (default value)
      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        assertAllColumnsAreLongButBigIntIsBigDecimal(rs);
      }

      // Test with JDBC_TREAT_DECIMAL_AS_INT=false
      stmt.execute(setJdbcTreatDecimalAsIntFalse);
      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        assertAllColumnsAreBigDecimal(rs);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetObjectWithType(String queryResultFormat) throws SQLException {
    try (Statement statement = createStatement(queryResultFormat)) {
      statement.execute(
          " CREATE OR REPLACE TABLE test_all_types ("
              + "                  string VARCHAR, "
              + "                  b TINYINT, "
              + "                  s SMALLINT, "
              + "                  i INTEGER, "
              + "                  l BIGINT, "
              + "                  f FLOAT, "
              + "                  d DOUBLE, "
              + "                  bd DOUBLE, "
              + "                  bool BOOLEAN, "
              + "                  timestampLtz TIMESTAMP_LTZ, "
              + "                  timestampNtz TIMESTAMP_NTZ, "
              + "                  timestampTz TIMESTAMP_TZ, "
              + "                  date DATE,"
              + "                  time TIME "
              + "                  )");
      statement.execute(
          "insert into test_all_types values('aString',1,2,3,4,1.1,2.2,3.3, false, "
              + "'2021-12-22 09:43:44','2021-12-22 09:43:44','2021-12-22 09:43:44', "
              + "'2023-12-24','12:34:56')");

      assertResultValueAndType(statement, "aString", "string", String.class);
      assertResultValueAndType(statement, new Byte("1"), "b", Byte.class);
      assertResultValueAndType(statement, Short.valueOf("2"), "s", Short.class);
      assertResultValueAndType(statement, Integer.valueOf("2"), "s", Integer.class);
      assertResultValueAndType(statement, Integer.valueOf("3"), "i", Integer.class);
      assertResultValueAndType(statement, Long.valueOf("4"), "l", Long.class);
      assertResultValueAndType(statement, BigDecimal.valueOf(4), "l", BigDecimal.class);
      assertResultValueAndType(statement, Float.valueOf("1.1"), "f", Float.class);
      assertResultValueAndType(statement, Double.valueOf("1.1"), "f", Double.class);
      assertResultValueAndType(statement, Double.valueOf("2.2"), "d", Double.class);
      assertResultValueAndType(statement, BigDecimal.valueOf(3.3), "bd", BigDecimal.class);
      assertResultValueAndType(statement, "FALSE", "bool", String.class);
      assertResultValueAndType(statement, Boolean.FALSE, "bool", Boolean.class);
      assertResultValueAndType(statement, 0L, "bool", Long.class);
      assertResultValueAsString(
          statement,
          new SnowflakeTimestampWithTimezone(
              Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)), TimeZone.getDefault()),
          "timestampLtz",
          Timestamp.class);
      assertResultValueAsString(
          statement,
          new SnowflakeTimestampWithTimezone(
              Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)), TimeZone.getDefault()),
          "timestampNtz",
          Timestamp.class);
      assertResultValueAsString(
          statement,
          new SnowflakeTimestampWithTimezone(
              Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)), TimeZone.getDefault()),
          "timestampTz",
          Timestamp.class);
      assertResultValueAndType(
          statement, Date.valueOf(LocalDate.of(2023, 12, 24)), "date", Date.class);
      assertResultValueAndType(
          statement, Time.valueOf(LocalTime.of(12, 34, 56)), "time", Time.class);
    }
  }

  private void assertResultValueAndType(
      Statement statement, Object expected, String columnName, Class<?> type) throws SQLException {
    try (ResultSet resultSetString =
        statement.executeQuery(String.format("select %s from test_all_types", columnName))) {
      assertTrue(resultSetString.next());
      assertEquals(expected, resultSetString.getObject(1, type));
    }
  }

  private void assertResultValueAsString(
      Statement statement, Object expected, String columnName, Class type) throws SQLException {
    try (ResultSet resultSetString =
        statement.executeQuery(String.format("select %s from test_all_types", columnName))) {
      assertTrue(resultSetString.next());
      assertEquals(expected.toString(), resultSetString.getObject(1, type).toString());
    }
  }
}
