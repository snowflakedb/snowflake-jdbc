/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.TestUtil;
import net.snowflake.client.category.TestCategoryResultSet;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SessionUtil;
import net.snowflake.client.jdbc.telemetry.*;
import net.snowflake.common.core.SFBinary;
import org.apache.arrow.vector.Float8Vector;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * ResultSet integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the oldest supported driver to examine
 * if the tests still is not applicable. If it is applicable, move tests to ResultSetIT so that both
 * the latest and oldest supported driver run the tests.
 */
@Category(TestCategoryResultSet.class)
public class ResultSetLatestIT extends ResultSet0IT {

  public ResultSetLatestIT() {
    this("json");
  }

  ResultSetLatestIT(String queryResultFormat) {
    super(queryResultFormat);
  }

  /**
   * Test that when closing of results is interrupted by Thread.Interrupt(), the memory is released
   * safely before driver execution ends.
   *
   * @throws Throwable
   */
  @Test
  public void testMemoryClearingAfterInterrupt() throws Throwable {
    ResultSet resultSet = null;
    final Connection connection = getConnection();
    final Statement statement = connection.createStatement();
    final long initialMemoryUsage = SnowflakeChunkDownloader.getCurrentMemoryUsage();
    try {
      // Inject an InterruptedException into the SnowflakeChunkDownloader.terminate() function
      SnowflakeChunkDownloader.setInjectedDownloaderException(new InterruptedException());
      // 10000 rows should be enough to force result into multiple chunks
      resultSet =
          statement.executeQuery(
              "select seq8(), randstr(1000, random()) from table(generator(rowcount => 10000))");
      assertThat(
          "hold memory usage for the resultSet before close",
          SnowflakeChunkDownloader.getCurrentMemoryUsage() - initialMemoryUsage >= 0);
      // Result closure should catch InterruptedException and throw a SQLException after its caught
      resultSet.close();
      fail("Exception should have been thrown");
    } catch (SQLException ex) {
      assertEquals((int) ErrorCode.INTERRUPTED.getMessageCode(), ex.getErrorCode());
      // Assert all memory was released
      assertThat(
          "closing statement didn't release memory allocated for result",
          SnowflakeChunkDownloader.getCurrentMemoryUsage(),
          equalTo(initialMemoryUsage));
    }
    // Unset the exception injection so statement and connection can close without exceptions
    SnowflakeChunkDownloader.setInjectedDownloaderException(null);
    closeSQLObjects(resultSet, statement, connection);
  }

  /**
   * This tests that the SnowflakeChunkDownloader doesn't hang when memory limits are low. Opening
   * multiple statements concurrently uses a lot of memory. This checks that chunks download even
   * when there is not enough memory available for concurrent prefetching.
   */
  @Test
  public void testChunkDownloaderNoHang() throws SQLException {
    int stmtCount = 30;
    int rowCount = 170000;
    Connection connection = getConnection();
    List<ResultSet> rsList = new ArrayList<>();
    // Set memory limit to low number
    connection
        .unwrap(SnowflakeConnectionV1.class)
        .getSFBaseSession()
        .setMemoryLimitForTesting(2000000);
    // open multiple statements concurrently to overwhelm current memory allocation
    for (int i = 0; i < stmtCount; ++i) {
      Statement stmt = connection.createStatement();
      ResultSet resultSet =
          stmt.executeQuery(
              "select randstr(100, random()) from table(generator(rowcount => " + rowCount + "))");
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
    connection.close();
  }

  /** This tests that the SnowflakeChunkDownloader doesn't hang when memory limits are low. */
  @Test
  public void testChunkDownloaderSetRetry() throws SQLException {
    int stmtCount = 3;
    int rowCount = 170000;
    Connection connection = getConnection();
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
      Statement stmt = connection.createStatement();
      ResultSet resultSet =
          stmt.executeQuery(
              "select randstr(100, random()) from table(generator(rowcount => " + rowCount + "))");
      // consume half of the results and go to the next statement
      for (int j = 0; j < rowCount / 2; j++) {
        resultSet.next();
      }
      assertTrue(Pattern.matches("[a-zA-Z0-9]{100}", resultSet.getString(1)));
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
    connection.close();
  }

  /**
   * Metadata API metric collection. The old driver didn't collect metrics.
   *
   * @throws SQLException arises if any exception occurs
   * @throws ExecutionException arises if error occurred when sending telemetry events
   * @throws InterruptedException arises if error occurred when sending telemetry events
   */
  @Test
  public void testMetadataAPIMetricCollection()
      throws SQLException, ExecutionException, InterruptedException {
    Connection con = init();
    Telemetry telemetry =
        con.unwrap(SnowflakeConnectionV1.class).getSfSession().getTelemetryClient();
    DatabaseMetaData metadata = con.getMetaData();
    // Call one of the DatabaseMetadata API functions but for simplicity, ensure returned ResultSet
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

    String catalog = con.getCatalog();
    String schema = con.getSchema();
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
  @Test
  public void testGetCharacterStreamNull() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table JDBC_NULL_CHARSTREAM (col1 varchar(16))");
    statement.execute("insert into JDBC_NULL_CHARSTREAM values(NULL)");
    ResultSet rs = statement.executeQuery("select * from JDBC_NULL_CHARSTREAM");
    rs.next();
    assertNull(rs.getCharacterStream(1));
    rs.close();
    connection.close();
  }

  /**
   * Large result chunk metrics tests. The old driver didn't collect metrics.
   *
   * @throws SQLException arises if any exception occurs
   */
  @Test
  public void testMultipleChunks() throws Exception {
    Connection con = init();
    Statement statement = con.createStatement();

    // 10000 rows should be enough to force result into multiple chunks
    ResultSet resultSet =
        statement.executeQuery(
            "select seq8(), randstr(1000, random()) from table(generator(rowcount => 10000))");
    int cnt = 0;
    while (resultSet.next()) {
      ++cnt;
    }
    assertTrue(cnt >= 0);
    Telemetry telemetry =
        con.unwrap(SnowflakeConnectionV1.class).getSfSession().getTelemetryClient();
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
        if (log.getMessage().get(TelemetryUtil.TYPE).textValue().equals(expectedFields[i].field)) {
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

  /**
   * Result set metadata
   *
   * @throws SQLException arises if any exception occurs
   */
  @Test
  public void testResultSetMetadata() throws SQLException {
    Connection connection = init();
    final Map<String, String> params = getConnectionParameters();
    Statement statement = connection.createStatement();

    statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
    statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
    ResultSet resultSet = statement.executeQuery("select * from test_rsmd");
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertEquals(
        params.get("database").toUpperCase(), resultSetMetaData.getCatalogName(1).toUpperCase());
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

    statement.execute("drop table if exists test_rsmd");
    statement.close();
    connection.close();
  }

  /**
   * Tests behavior of empty ResultSet. Only callable from getGeneratedKeys().
   *
   * @throws SQLException
   */
  @Test
  public void testEmptyResultSet() throws SQLException {
    Connection con = init();
    Statement statement = con.createStatement();
    // the only function that returns ResultSetV1.emptyResultSet()
    ResultSet rs = statement.getGeneratedKeys();
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
    rs.close();
    assertTrue(rs.isClosed());
    statement.close();
    con.close();
  }

  /**
   * Gets bytes from other data types
   *
   * @throws Exception arises if any exception occurs.
   */
  @Test
  public void testBytesCrossTypeTests() throws Exception {
    ResultSet resultSet = numberCrossTesting();
    resultSet.next();
    // assert that 0 is returned for null values for every type of value
    for (int i = 1; i < 13; i++) {
      assertArrayEquals(null, resultSet.getBytes(i));
    }
    resultSet.next();
    assertArrayEquals(intToByteArray(2), resultSet.getBytes(1));
    assertArrayEquals(intToByteArray(5), resultSet.getBytes(2));
    assertArrayEquals(floatToByteArray(3.5f), resultSet.getBytes(3));
    assertArrayEquals(new byte[] {1}, resultSet.getBytes(4));
    assertArrayEquals(new byte[] {(byte) '1'}, resultSet.getBytes(5));
    assertArrayEquals("1".getBytes(), resultSet.getBytes(6));

    for (int i = 7; i < 12; i++) {
      try {
        resultSet.getBytes(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }

    byte[] decoded = SFBinary.fromHex("48454C4C4F").getBytes();

    assertArrayEquals(decoded, resultSet.getBytes(12));
  }

  // SNOW-204185
  // 30s for timeout. This test usually finishes in around 10s.
  @Test(timeout = 30000)
  public void testResultChunkDownloaderException() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();

    // The generated resultSet must be big enough for triggering result chunk downloader
    String query =
        "select current_date(), true,2345234, 2343.0, 'testrgint\\n"
            + "\\t' from table(generator(rowcount=>10000))";

    ResultSet resultSet = statement.executeQuery(query);
    resultSet.next(); // should finish successfully

    try {
      SnowflakeChunkDownloader.setInjectedDownloaderException(
          new OutOfMemoryError("Fake OOM error for testing"));
      resultSet = statement.executeQuery(query);
      try {
        // Normally this step won't cause too long. Because we will get exception once trying to get
        // result from the first chunk downloader
        while (resultSet.next())
          ;
        fail("Should not reach here. Last next() command is supposed to throw an exception");
      } catch (SnowflakeSQLException ex) {
        // pass, do nothing
      }
    } finally {
      SnowflakeChunkDownloader.setInjectedDownloaderException(null);
    }

    statement.close();
    connection.close();
  }

  /**
   * SNOW-165204 Fix exception that resulted from fetching too large a number with getObject().
   *
   * @throws SQLException
   */
  @Test
  public void testGetObjectWithBigInt() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    statement.execute("alter session set jdbc_query_result_format ='json'");
    // test with greatest possible number and greatest negative possible number
    String[] extremeNumbers = {
      "99999999999999999999999999999999999999", "-99999999999999999999999999999999999999"
    };
    for (int i = 0; i < extremeNumbers.length; i++) {
      ResultSet resultSet = statement.executeQuery("select " + extremeNumbers[i]);
      resultSet.next();
      assertEquals(Types.BIGINT, resultSet.getMetaData().getColumnType(1));
      assertEquals(new BigDecimal(extremeNumbers[i]), resultSet.getObject(1));
    }
    statement.close();
    connection.close();
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
  @Test
  public void testGetBigDecimalWithScale() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_get(colA number(38,9))");
    PreparedStatement preparedStatement =
        connection.prepareStatement("insert into test_get values(?)");
    preparedStatement.setBigDecimal(1, null);
    preparedStatement.addBatch();
    BigDecimal bigDecimal = new BigDecimal("100000000.123456789");
    preparedStatement.setBigDecimal(1, bigDecimal);
    preparedStatement.addBatch();
    preparedStatement.executeBatch();

    ResultSet resultSet = statement.executeQuery("select * from test_get");
    resultSet.next();
    assertEquals(null, resultSet.getBigDecimal(1, 5));
    assertEquals(null, resultSet.getBigDecimal("COLA", 5));
    resultSet.next();
    assertEquals(bigDecimal.setScale(5, RoundingMode.HALF_UP), resultSet.getBigDecimal(1, 5));
    assertEquals(bigDecimal.setScale(5, RoundingMode.HALF_UP), resultSet.getBigDecimal("COLA", 5));
  }

  @Test
  public void testGetDataTypeWithTimestampTz() throws Exception {
    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      statement.executeQuery("create or replace table ts_test(ts timestamp_tz)");
      ResultSet resultSet = statement.executeQuery("select * from ts_test");
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      // Assert that TIMESTAMP_TZ type matches java.sql.TIMESTAMP_WITH_TIMEZONE
      assertEquals(resultSetMetaData.getColumnType(1), 2014);
      // Assert that TIMESTAMP_TZ column returns Timestamp class name
      assertEquals(resultSetMetaData.getColumnClassName(1), Timestamp.class.getName());

      SFBaseSession baseSession = connection.unwrap(SnowflakeConnectionV1.class).getSFBaseSession();
      Field field = SFBaseSession.class.getDeclaredField("enableReturnTimestampWithTimeZone");
      field.setAccessible(true);
      field.set(baseSession, false);

      statement = connection.createStatement();
      resultSet = statement.executeQuery("select * from ts_test");
      resultSetMetaData = resultSet.getMetaData();
      // Assert that TIMESTAMP_TZ type matches java.sql.TIMESTAMP when
      // enableReturnTimestampWithTimeZone is false.
      assertEquals(resultSetMetaData.getColumnType(1), Types.TIMESTAMP);
    }
  }

  /**
   * Test getClob(int), and getClob(String) handle SQL nulls and don't throw a NullPointerException
   * (SNOW-749517)
   *
   * @throws SQLException
   */
  @Test
  public void testGetEmptyOrNullClob() throws SQLException {
    Connection connection = init();
    Clob clob = connection.createClob();
    clob.setString(1, "hello world");
    Clob emptyClob = connection.createClob();
    emptyClob.setString(1, "");
    Statement statement = connection.createStatement();
    statement.execute(
        "create or replace table test_get_clob(colA varchar, colNull varchar, colEmpty text)");
    PreparedStatement preparedStatement =
        connection.prepareStatement("insert into test_get_clob values(?, ?, ?)");
    preparedStatement.setClob(1, clob);
    preparedStatement.setString(2, null);
    preparedStatement.setClob(3, emptyClob);
    preparedStatement.execute();

    ResultSet resultSet = statement.executeQuery("select * from test_get_clob");
    resultSet.next();
    assertEquals("hello world", resultSet.getClob(1).toString());
    assertEquals("hello world", resultSet.getClob("COLA").toString());
    assertNull(resultSet.getClob(2));
    assertNull(resultSet.getClob("COLNULL"));
    assertEquals("", resultSet.getClob(3).toString());
    assertEquals("", resultSet.getClob("COLEMPTY").toString());
  }

  /**
   * Since now getClob(x) can return a null, theoretically someone may work with a null Clob and try
   * to use the setClob(int, Clob) method which will result in a NullPointerException. (SNOW-749517)
   *
   * @throws SQLException
   */
  @Test
  public void testSetNullClob() throws SQLException {
    Connection connection = init();
    Clob clob = null;
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_set_clob(colNull varchar)");
    PreparedStatement preparedStatement =
        connection.prepareStatement("insert into test_set_clob values(?)");
    preparedStatement.setClob(1, clob);
    preparedStatement.execute();

    ResultSet resultSet = statement.executeQuery("select * from test_set_clob");
    resultSet.next();
    assertNull(resultSet.getClob(1));
    assertNull(resultSet.getClob("COLNULL"));
  }

  @Test
  public void testCallStatementType() throws SQLException {
    Properties props = new Properties();
    props.put("USE_STATEMENT_TYPE_CALL_FOR_STORED_PROC_CALLS", "true");
    try (Connection connection = getConnection(props)) {
      try (Statement statement = connection.createStatement()) {
        String sp =
            "CREATE OR REPLACE PROCEDURE \"SP_ZSDLEADTIME_ARCHIVE_DAILY\"()\n"
                + "RETURNS VARCHAR(16777216)\n"
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

        CallableStatement cs = connection.prepareCall("CALL SP_ZSDLEADTIME_ARCHIVE_DAILY()");
        cs.execute();
        ResultSetMetaData resultSetMetaData = cs.getMetaData();
        assertEquals("SP_ZSDLEADTIME_ARCHIVE_DAILY", resultSetMetaData.getColumnName(1));
        assertEquals("VARCHAR", resultSetMetaData.getColumnTypeName(1));
        assertEquals(0, resultSetMetaData.getScale(1));
        assertEquals(16777216, resultSetMetaData.getPrecision(1));

        cs.close();
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
  @Test
  public void testNewFeaturesNotSupported() throws SQLException {
    Connection con = init();
    ResultSet rs = con.createStatement().executeQuery("select 1");
    try {
      rs.unwrap(SnowflakeResultSet.class).getQueryErrorMessage();
    } catch (SQLFeatureNotSupportedException ex) {
      // catch SQLFeatureNotSupportedException
      assertEquals("This function is only supported for asynchronous queries.", ex.getMessage());
    }
    rs.close();
    con.close();
  }

  @Test
  public void testGetObjectJsonResult() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    statement.execute("alter session set jdbc_query_result_format ='json'");
    statement.execute("create or replace table testObj (colA double, colB boolean)");

    PreparedStatement preparedStatement =
        connection.prepareStatement("insert into testObj values(?, ?)");
    preparedStatement.setDouble(1, 22.2);
    preparedStatement.setBoolean(2, true);
    preparedStatement.executeQuery();

    ResultSet resultSet = statement.executeQuery("select * from testObj");
    resultSet.next();
    assertEquals(22.2, resultSet.getObject(1));
    assertEquals(true, resultSet.getObject(2));

    statement.execute("drop table if exists testObj");
    statement.close();
    connection.close();
  }

  @Test
  public void testMetadataIsCaseSensitive() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();

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
    ResultSet rs = statement.executeQuery("select * from case_sensitive");
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

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testAutoIncrementJsonResult() throws SQLException {
    Properties paramProperties = new Properties();
    paramProperties.put("ENABLE_FIX_759900", true);
    Connection connection = init(paramProperties);
    Statement statement = connection.createStatement();
    statement.execute("alter session set jdbc_query_result_format ='json'");

    statement.execute(
        "create or replace table auto_inc(id int autoincrement, name varchar(10), another_col int autoincrement)");
    statement.execute("insert into auto_inc(name) values('test1')");

    ResultSet resultSet = statement.executeQuery("select * from auto_inc");
    resultSet.next();

    ResultSetMetaData metaData = resultSet.getMetaData();
    assertTrue(metaData.isAutoIncrement(1));
    assertFalse(metaData.isAutoIncrement(2));
    assertTrue(metaData.isAutoIncrement(3));
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testAutoIncrementArrowResult() throws SQLException {
    Properties paramProperties = new Properties();
    paramProperties.put("ENABLE_FIX_759900", true);
    Connection connection = init(paramProperties);
    Statement statement = connection.createStatement();
    statement.execute("alter session set jdbc_query_result_format ='arrow'");

    statement.execute(
        "create or replace table auto_inc(id int autoincrement, name varchar(10), another_col int autoincrement)");
    statement.execute("insert into auto_inc(name) values('test1')");

    ResultSet resultSet = statement.executeQuery("select * from auto_inc");
    resultSet.next();

    ResultSetMetaData metaData = resultSet.getMetaData();
    assertTrue(metaData.isAutoIncrement(1));
    assertFalse(metaData.isAutoIncrement(2));
    assertTrue(metaData.isAutoIncrement(3));
  }

  @Test
  public void testGranularTimeFunctionsInSessionTimezone() throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      connection = getConnection();
      statement = connection.createStatement();
      statement.execute("create or replace table testGranularTime(t time)");
      statement.execute("insert into testGranularTime values ('10:10:10')");
      ResultSet resultSet = statement.executeQuery("select * from testGranularTime");
      resultSet.next();
      assertEquals(Time.valueOf("10:10:10"), resultSet.getTime(1));
      assertEquals(10, resultSet.getTime(1).getHours());
      assertEquals(10, resultSet.getTime(1).getMinutes());
      assertEquals(10, resultSet.getTime(1).getSeconds());
      resultSet.close();
    } finally {
      statement.execute("drop table if exists testGranularTime");
      statement.close();
      connection.close();
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGranularTimeFunctionsInUTC() throws SQLException {
    Connection connection = null;
    Statement statement = null;
    TimeZone origTz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    try {
      connection = getConnection();
      statement = connection.createStatement();
      statement.execute("alter session set JDBC_USE_SESSION_TIMEZONE=false");
      statement.execute("create or replace table testGranularTime(t time)");
      statement.execute("insert into testGranularTime values ('10:10:10')");
      ResultSet resultSet = statement.executeQuery("select * from testGranularTime");
      resultSet.next();
      assertEquals(Time.valueOf("02:10:10"), resultSet.getTime(1));
      assertEquals(02, resultSet.getTime(1).getHours());
      assertEquals(10, resultSet.getTime(1).getMinutes());
      assertEquals(10, resultSet.getTime(1).getSeconds());
      resultSet.close();
    } finally {
      TimeZone.setDefault(origTz);
      statement.execute("drop table if exists testGranularTime");
      statement.close();
      connection.close();
    }
  }
}
