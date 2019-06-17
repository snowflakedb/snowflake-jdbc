/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnTravisCI;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import net.snowflake.client.jdbc.telemetry.TelemetryField;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test ResultSet
 */
public class ResultSetIT extends BaseJDBCTest
{
  private final String selectAllSQL = "select * from test_rs";

  public static Connection getConnection(int injectSocketTimeout)
  throws SQLException
  {
    Connection connection = BaseJDBCTest.getConnection(injectSocketTimeout);

    Statement statement = connection.createStatement();
    statement.execute(
        "alter session set " +
        "TIMEZONE='America/Los_Angeles'," +
        "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ'," +
        "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'," +
        "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'," +
        "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'," +
        "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
    statement.close();
    return connection;
  }

  public static Connection getConnection()
  throws SQLException
  {
    return getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
  }


  @Before
  public void setUp() throws SQLException
  {
    Connection con = getConnection();

    // TEST_RS
    con.createStatement().execute("create or replace table test_rs (colA string)");
    con.createStatement().execute("insert into test_rs values('rowOne')");
    con.createStatement().execute("insert into test_rs values('rowTwo')");
    con.createStatement().execute("insert into test_rs values('rowThree')");

    // ORDERS_JDBC
    Statement statement = con.createStatement();
    statement.execute("create or replace table orders_jdbc" +
                      "(C1 STRING NOT NULL COMMENT 'JDBC', "
                      + "C2 STRING, C3 STRING, C4 STRING, C5 STRING, C6 STRING, "
                      + "C7 STRING, C8 STRING, C9 STRING) "
                      + "stage_file_format = (field_delimiter='|' "
                      + "error_on_column_count_mismatch=false)");
    // put files
    assertTrue("Failed to put a file",
               statement.execute(
                   "PUT file://" +
                   getFullPathFileInResource(TEST_DATA_FILE) + " @%orders_jdbc"));
    assertTrue("Failed to put a file",
               statement.execute(
                   "PUT file://" +
                   getFullPathFileInResource(TEST_DATA_FILE_2) + " @%orders_jdbc"));

    int numRows =
        statement.executeUpdate("copy into orders_jdbc");

    assertEquals("Unexpected number of rows copied: " + numRows, 73, numRows);


    con.close();
  }

  @After
  public void tearDown() throws SQLException
  {
    Connection con = getConnection();
    con.createStatement().execute("drop table if exists orders_jdbc");
    con.createStatement().execute("drop table if exists test_rs");
    con.close();
  }

  @Test
  public void testFindColumn() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(selectAllSQL);
    assertEquals(1, resultSet.findColumn("COLA"));
    statement.close();
    connection.close();
  }

  @Test
  public void testGetMethod() throws Throwable
  {
    String prepInsertString = "insert into test_get values(?, ?, ?, ?, ?, ?, ?, ?)";
    int bigInt = Integer.MAX_VALUE;
    long bigLong = Long.MAX_VALUE;
    short bigShort = Short.MAX_VALUE;
    String str = "hello";
    double bigDouble = Double.MAX_VALUE;
    float bigFloat = Float.MAX_VALUE;

    Connection connection = getConnection();
    Clob clob = connection.createClob();
    clob.setString(1, "hello world");
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_get(colA integer, colB number, colC number, "
                      + "colD string, colE double, colF float, colG boolean, colH text)");

    PreparedStatement prepStatement = connection.prepareStatement(prepInsertString);
    prepStatement.setInt(1, bigInt);
    prepStatement.setLong(2, bigLong);
    prepStatement.setLong(3, bigShort);
    prepStatement.setString(4, str);
    prepStatement.setDouble(5, bigDouble);
    prepStatement.setFloat(6, bigFloat);
    prepStatement.setBoolean(7, true);
    prepStatement.setClob(8, clob);
    prepStatement.execute();

    statement.execute("select * from test_get");
    ResultSet resultSet = statement.getResultSet();
    resultSet.next();
    assertEquals(bigInt, resultSet.getInt(1));
    assertEquals(bigInt, resultSet.getInt("COLA"));
    assertEquals(bigLong, resultSet.getLong(2));
    assertEquals(bigLong, resultSet.getLong("COLB"));
    assertEquals(bigShort, resultSet.getShort(3));
    assertEquals(bigShort, resultSet.getShort("COLC"));
    assertEquals(str, resultSet.getString(4));
    assertEquals(str, resultSet.getString("COLD"));
    Reader reader = resultSet.getCharacterStream("COLD");
    char[] sample = new char[str.length()];

    assertEquals(str.length(), reader.read(sample));
    assertEquals(str.charAt(0), sample[0]);
    assertEquals(str, new String(sample));

    //assertEquals(bigDouble, resultSet.getDouble(5), 0);
    //assertEquals(bigDouble, resultSet.getDouble("COLE"), 0);
    assertEquals(bigFloat, resultSet.getFloat(6), 0);
    assertEquals(bigFloat, resultSet.getFloat("COLF"), 0);
    assertTrue(resultSet.getBoolean(7));
    assertTrue(resultSet.getBoolean("COLG"));
    assertEquals("hello world", resultSet.getClob("COLH").toString());

    //test getStatement method
    assertEquals(statement, resultSet.getStatement());

    prepStatement.close();
    statement.execute("drop table if exists table_get");
    statement.close();
    resultSet.close();
    connection.close();
  }

  @Test
  public void testGetObjectOnDatabaseMetadataResultSet()
  throws SQLException
  {
    Connection connection = getConnection();
    DatabaseMetaData databaseMetaData = connection.getMetaData();
    ResultSet resultSet = databaseMetaData.getTypeInfo();
    resultSet.next();
    // SNOW-21375 "NULLABLE" Column is a SMALLINT TYPE
    assertEquals(DatabaseMetaData.typeNullable, resultSet.getObject("NULLABLE"));
    resultSet.close();
    connection.close();
  }

  @Test
  public void testGetBigDecimal() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_get(colA number(38,9))");
    PreparedStatement preparedStatement = connection.prepareStatement(
        "insert into test_get values(?)");
    BigDecimal bigDecimal1 = new BigDecimal("10000000000");
    preparedStatement.setBigDecimal(1, bigDecimal1);
    preparedStatement.executeUpdate();

    BigDecimal bigDecimal2 = new BigDecimal("100000000.123456789");
    preparedStatement.setBigDecimal(1, bigDecimal2);
    preparedStatement.execute();

    statement.execute("select * from test_get order by 1");
    ResultSet resultSet = statement.getResultSet();
    resultSet.next();
    assertEquals(bigDecimal2, resultSet.getBigDecimal(1));
    assertEquals(bigDecimal2, resultSet.getBigDecimal("COLA"));

    preparedStatement.close();
    statement.execute("drop table if exists test_get");
    statement.close();
    resultSet.close();
    connection.close();
  }

  @Test
  public void testCursorPosition() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute(selectAllSQL);
    ResultSet resultSet = statement.getResultSet();
    resultSet.next();
    assertTrue(resultSet.isFirst());
    assertEquals(1, resultSet.getRow());
    resultSet.next();
    assertTrue(!resultSet.isFirst());
    assertEquals(2, resultSet.getRow());
    assertTrue(!resultSet.isLast());
    resultSet.next();
    assertEquals(3, resultSet.getRow());
    assertTrue(resultSet.isLast());
    resultSet.next();
    assertTrue(resultSet.isAfterLast());
    statement.close();
    connection.close();
  }

  @Test
  public void testGetDateAndTime() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute(
        "create or replace table dateTime(colA Date, colB Timestamp, colC Time)");

    java.util.Date today = new java.util.Date();
    Date date = buildDate(2016, 3, 20);
    Timestamp ts = new Timestamp(today.getTime());
    Time tm = new Time(12345678); // 03:25:45.678
    final String insertTime = "insert into datetime values(?, ?, ?)";
    PreparedStatement prepStatement = connection.prepareStatement(insertTime);
    prepStatement.setDate(1, date);
    prepStatement.setTimestamp(2, ts);
    prepStatement.setTime(3, tm);

    prepStatement.execute();

    ResultSet resultSet = statement.executeQuery("select * from datetime");
    resultSet.next();
    assertEquals(date, resultSet.getDate(1));
    assertEquals(date, resultSet.getDate("COLA"));
    assertEquals(ts, resultSet.getTimestamp(2));
    assertEquals(ts, resultSet.getTimestamp("COLB"));
    assertEquals(tm, resultSet.getTime(3));
    assertEquals(tm, resultSet.getTime("COLC"));

    statement.execute("create or replace table datetime(colA timestamp_ltz, colB timestamp_ntz, colC timestamp_tz)");
    statement.execute("insert into dateTime values ('2019-01-01 17:17:17', '2019-01-01 17:17:17', '2019-01-01 " +
                      "17:17:17')");
    prepStatement =
        connection.prepareStatement("insert into datetime values(?, '2019-01-01 17:17:17', '2019-01-01 17:17:17')");
    Timestamp dateTime = new Timestamp(date.getTime());
    prepStatement.setTimestamp(1, dateTime);
    prepStatement.execute();
    resultSet = statement.executeQuery("select * from datetime");
    resultSet.next();
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    formatter.setTimeZone(TimeZone.getDefault());
    String d = formatter.format(resultSet.getDate("COLA"));
    assertEquals("2019-01-02 01:17:17", d);
    resultSet.next();
    assertEquals(date, resultSet.getDate(1));
    assertEquals(date, resultSet.getDate("COLA"));
    statement.execute("drop table if exists datetime");
    connection.close();
  }

  // SNOW-25029: The driver should reduce Time milliseconds mod 24h.
  @Test
  public void testTimeRange() throws SQLException
  {
    final String insertTime = "insert into timeTest values (?), (?), (?), (?)";
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table timeTest (c1 time)");

    long ms1 = -2202968667333L; // 1900-03-11 09:15:33.667
    long ms2 = -1;              // 1969-12-31 23:59:99.999
    long ms3 = 86400 * 1000;    // 1970-01-02 00:00:00
    long ms4 = 1451680250123L;  // 2016-01-01 12:30:50.123

    Time tm1 = new Time(ms1);
    Time tm2 = new Time(ms2);
    Time tm3 = new Time(ms3);
    Time tm4 = new Time(ms4);

    PreparedStatement prepStatement = connection.prepareStatement(insertTime);
    prepStatement.setTime(1, tm1);
    prepStatement.setTime(2, tm2);
    prepStatement.setTime(3, tm3);
    prepStatement.setTime(4, tm4);

    prepStatement.execute();

    // Note that the resulting Time objects are NOT equal because they have
    // their milliseconds in the range 0 to 86,399,999, i.e. inside Jan 1, 1970.
    // PreparedStatement accepts Time objects outside this range, but it reduces
    // modulo 24 hours to discard the date information before sending to GS.

    final long M = 86400 * 1000;
    ResultSet resultSet = statement.executeQuery("select * from timeTest");
    resultSet.next();
    assertNotEquals(tm1, resultSet.getTime(1));
    assertEquals(new Time((ms1 % M + M) % M), resultSet.getTime(1));
    resultSet.next();
    assertNotEquals(tm2, resultSet.getTime(1));
    assertEquals(new Time((ms2 % M + M) % M), resultSet.getTime(1));
    resultSet.next();
    assertNotEquals(tm3, resultSet.getTime(1));
    assertEquals(new Time((ms3 % M + M) % M), resultSet.getTime(1));
    resultSet.next();
    assertNotEquals(tm4, resultSet.getTime(1));
    assertEquals(new Time((ms4 % M + M) % M), resultSet.getTime(1));

    statement.execute("drop table if exists timeTest");
    connection.close();
  }

  @Test
  public void testCurrentTime() throws SQLException
  {
    final String insertTime = "insert into datetime values (?, ?, ?)";
    Connection connection = getConnection();

    assertFalse(connection.createStatement().
        execute("alter session set TIMEZONE='UTC'"));

    Statement statement = connection.createStatement();
    statement.execute(
        "create or replace table datetime (d date, ts timestamp, tm time)");
    PreparedStatement prepStatement = connection.prepareStatement(insertTime);

    long currentMillis = System.currentTimeMillis();
    Date currentDate = new Date(currentMillis);
    Timestamp currentTS = new Timestamp(currentMillis);
    Time currentTime = new Time(currentMillis);

    prepStatement.setDate(1, currentDate);
    prepStatement.setTimestamp(2, currentTS);
    prepStatement.setTime(3, currentTime);

    prepStatement.execute();

    ResultSet resultSet = statement.executeQuery("select ts::date = d from datetime");
    resultSet.next();
    assertTrue(resultSet.getBoolean(1));
    resultSet = statement.executeQuery("select ts::time = tm from datetime");
    resultSet.next();
    assertTrue(resultSet.getBoolean(1));

    statement.execute("drop table if exists datetime");
    connection.close();
  }


  @Test
  public void testBindTimestampTZ() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute(
        "create or replace table testBindTimestampTZ(" +
        "cola int, colb timestamp_tz)");

    long millSeconds = System.currentTimeMillis();
    Timestamp ts = new Timestamp(millSeconds);
    PreparedStatement prepStatement = connection.prepareStatement(
        "insert into testBindTimestampTZ values (?, ?)");
    prepStatement.setInt(1, 123);
    prepStatement.setTimestamp(2, ts, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
    prepStatement.execute();

    ResultSet resultSet = statement.executeQuery(
        "select cola, colb from testBindTimestampTz");
    resultSet.next();
    assertThat("integer", resultSet.getInt(1), equalTo(123));
    assertThat("timestamp_tz", resultSet.getTimestamp(2), equalTo(ts));

    statement.execute("drop table if exists testBindTimestampTZ");
    connection.close();
  }

  @Test
  public void testGetBytes() throws SQLException
  {
    Properties props = new Properties();
    props.setProperty("enable_binary_datatype", "true");
    Connection connection = getConnection(props);
    Statement statement = connection.createStatement();
    statement.execute("create or replace table bin (b Binary)");

    byte[] bytes1 = new byte[0];
    byte[] bytes2 = {(byte) 0xAB, (byte) 0xCD, (byte) 0x12};
    byte[] bytes3 = {(byte) 0x00, (byte) 0xFF, (byte) 0x42, (byte) 0x01};

    PreparedStatement prepStatement = connection.prepareStatement(
        "insert into bin values (?), (?), (?)");
    prepStatement.setBytes(1, bytes1);
    prepStatement.setBytes(2, bytes2);
    prepStatement.setBytes(3, bytes3);
    prepStatement.execute();

    // Get results in hex format (default).
    ResultSet resultSet = statement.executeQuery("select * from bin");
    resultSet.next();
    assertArrayEquals(bytes1, resultSet.getBytes(1));
    assertEquals("", resultSet.getString(1));
    resultSet.next();
    assertArrayEquals(bytes2, resultSet.getBytes(1));
    assertEquals("ABCD12", resultSet.getString(1));
    resultSet.next();
    assertArrayEquals(bytes3, resultSet.getBytes(1));
    assertEquals("00FF4201", resultSet.getString(1));

    // Get results in base64 format.
    props.setProperty("binary_output_format", "BAse64");
    connection = getConnection(props);
    statement = connection.createStatement();
    resultSet = statement.executeQuery("select * from bin");
    resultSet.next();
    assertArrayEquals(bytes1, resultSet.getBytes(1));
    assertEquals("", resultSet.getString(1));
    resultSet.next();
    assertArrayEquals(bytes2, resultSet.getBytes(1));
    assertEquals("q80S", resultSet.getString(1));
    resultSet.next();
    assertArrayEquals(bytes3, resultSet.getBytes(1));
    assertEquals("AP9CAQ==", resultSet.getString(1));

    statement.execute("drop table if exists bin");
    connection.close();
  }

  @Test
  public void testResultSetMetadata() throws SQLException
  {
    Connection connection = getConnection();
    final Map<String, String> params = getConnectionParameters();
    Statement statement = connection.createStatement();

    statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
    statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");

    ResultSet resultSet = statement.executeQuery("select * from test_rsmd");
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

    assertEquals(params.get("database").toUpperCase(),
                 resultSetMetaData.getCatalogName(1).toUpperCase());
    assertEquals(params.get("schema").toUpperCase(),
                 resultSetMetaData.getSchemaName(1).toUpperCase());
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

    statement.execute("drop table if exists test_rsmd");
    statement.close();
    connection.close();
  }

  // SNOW-31647
  @Test
  public void testColumnMetaWithZeroPrecision() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    statement.execute("create or replace table testColDecimal(cola number(38, 0), " +
                      "colb number(17, 5))");

    ResultSet resultSet = statement.executeQuery("select * from testColDecimal");
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

    assertThat(resultSetMetaData.getColumnType(1), is(Types.BIGINT));
    assertThat(resultSetMetaData.getColumnType(2), is(Types.DECIMAL));
    assertThat(resultSetMetaData.isSigned(1), is(true));
    assertThat(resultSetMetaData.isSigned(2), is(true));


    statement.execute("drop table if exists testColDecimal");

    connection.close();
  }

  @Test
  public void testGetOldDate() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    statement.execute("create or replace table testOldDate(d date)");
    statement.execute("insert into testOldDate values ('0001-01-01'), " +
                      "(to_date('1000-01-01')), ('1300-01-01'), ('1400-02-02'), " +
                      "('1500-01-01'), ('1600-02-03')");

    ResultSet resultSet = statement.executeQuery("select * from testOldDate order by d");
    resultSet.next();
    assertEquals("0001-01-01", resultSet.getString(1));
    assertEquals(Date.valueOf("0001-01-01"), resultSet.getDate(1));
    resultSet.next();
    assertEquals("1000-01-01", resultSet.getString(1));
    assertEquals(Date.valueOf("1000-01-01"), resultSet.getDate(1));
    resultSet.next();
    assertEquals("1300-01-01", resultSet.getString(1));
    assertEquals(Date.valueOf("1300-01-01"), resultSet.getDate(1));
    resultSet.next();
    assertEquals("1400-02-02", resultSet.getString(1));
    assertEquals(Date.valueOf("1400-02-02"), resultSet.getDate(1));
    resultSet.next();
    assertEquals("1500-01-01", resultSet.getString(1));
    assertEquals(Date.valueOf("1500-01-01"), resultSet.getDate(1));
    resultSet.next();
    assertEquals("1600-02-03", resultSet.getString(1));
    assertEquals(Date.valueOf("1600-02-03"), resultSet.getDate(1));

    resultSet.close();
    statement.execute("drop table if exists testOldDate");
    statement.close();
    connection.close();
  }

  @Test
  public void testGetObjectOnFixedView() throws Exception
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    statement.execute(
        "create or replace table testFixedView" +
        "(C1 STRING NOT NULL COMMENT 'JDBC', "
        + "C2 STRING, C3 STRING, C4 STRING, C5 STRING, C6 STRING, "
        + "C7 STRING, C8 STRING, C9 STRING) "
        + "stage_file_format = (field_delimiter='|' "
        + "error_on_column_count_mismatch=false)");

    // put files
    assertTrue("Failed to put a file",
               statement.execute(
                   "PUT file://" +
                   getFullPathFileInResource(TEST_DATA_FILE) + " @%testFixedView"));

    ResultSet resultSet = statement.executeQuery(
        "PUT file://" +
        getFullPathFileInResource(TEST_DATA_FILE_2) + " @%testFixedView");

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    while (resultSet.next())
    {
      for (int i = 0; i < resultSetMetaData.getColumnCount(); i++)
      {
        assertNotNull(resultSet.getObject(i + 1));
      }
    }

    resultSet.close();
    statement.execute("drop table if exists testFixedView");
    statement.close();
    connection.close();
  }

  @Test
  public void testGetColumnDisplaySizeAndPrecision() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    ResultSet resultSet = statement.executeQuery("select cast(1 as char)");
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertEquals(1, resultSetMetaData.getColumnDisplaySize(1));
    assertEquals(1, resultSetMetaData.getPrecision(1));

    resultSet = statement.executeQuery("select cast(1 as number(38, 0))");
    resultSetMetaData = resultSet.getMetaData();
    assertEquals(39, resultSetMetaData.getColumnDisplaySize(1));
    assertEquals(38, resultSetMetaData.getPrecision(1));

    resultSet = statement.executeQuery("select cast(1 as decimal(25, 15))");
    resultSetMetaData = resultSet.getMetaData();
    assertEquals(27, resultSetMetaData.getColumnDisplaySize(1));
    assertEquals(25, resultSetMetaData.getPrecision(1));

    resultSet = statement.executeQuery("select cast(1 as string)");
    resultSetMetaData = resultSet.getMetaData();
    assertEquals(16777216, resultSetMetaData.getColumnDisplaySize(1));
    assertEquals(16777216, resultSetMetaData.getPrecision(1));

    resultSet = statement.executeQuery("select cast(1 as string(30))");
    resultSetMetaData = resultSet.getMetaData();
    assertEquals(30, resultSetMetaData.getColumnDisplaySize(1));
    assertEquals(30, resultSetMetaData.getPrecision(1));

    resultSet = statement.executeQuery("select to_date('2016-12-13', 'YYYY-MM-DD')");
    resultSetMetaData = resultSet.getMetaData();
    assertEquals(10, resultSetMetaData.getColumnDisplaySize(1));
    assertEquals(10, resultSetMetaData.getPrecision(1));

    resultSet = statement.executeQuery("select to_time('12:34:56', 'HH24:MI:SS')");
    resultSetMetaData = resultSet.getMetaData();
    assertEquals(8, resultSetMetaData.getColumnDisplaySize(1));
    assertEquals(8, resultSetMetaData.getPrecision(1));

    statement.close();
    connection.close();
  }

  @Test
  public void testGetBoolean() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table testBoolean(cola boolean)");
    statement.execute("insert into testBoolean values(false)");
    ResultSet resultSet = statement.executeQuery("select * from testBoolean");
    resultSet.next();
    assertFalse(resultSet.getBoolean(1));

    statement.execute("insert into testBoolean values(true)");
    resultSet = statement.executeQuery("select * from testBoolean");
    resultSet.next();
    assertFalse(resultSet.getBoolean(1));
    resultSet.next();
    assertTrue(resultSet.getBoolean(1));
    statement.execute("drop table if exists testBoolean");
    statement.close();
    connection.close();
  }

  @Test
  public void testGetClob() throws Throwable
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table testClob(cola text)");
    statement.execute("insert into testClob values('hello world')");
    statement.execute("insert into testClob values('hello world1')");
    statement.execute("insert into testClob values('hello world2')");
    statement.execute("insert into testClob values('hello world3')");
    ResultSet resultSet = statement.executeQuery("select * from testClob");
    resultSet.next();
    // test reading Clob
    char[] chars = new char[100];
    Reader reader = resultSet.getClob(1).getCharacterStream();
    int charRead;
    charRead = reader.read(chars, 0, chars.length);
    assertEquals(charRead, 11);
    assertEquals("hello world", resultSet.getClob(1).toString());

    // test reading truncated clob
    resultSet.next();
    Clob clob = resultSet.getClob(1);
    assertEquals(clob.length(), 12);
    clob.truncate(5);
    reader = clob.getCharacterStream();

    charRead = reader.read(chars, 0, chars.length);
    assertEquals(charRead, 5);

    // read from input stream
    resultSet.next();
    final InputStream input = resultSet.getClob(1).getAsciiStream();

    Reader in = new InputStreamReader(input, StandardCharsets.UTF_8);
    charRead = in.read(chars, 0, chars.length);
    assertEquals(charRead, 12);

    statement.close();
    connection.close();
  }

  @Test
  public void testFetchOnClosedResultSet() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(selectAllSQL);
    assertTrue(!resultSet.isClosed());
    resultSet.close();
    assertTrue(resultSet.isClosed());
    assertFalse(resultSet.next());
  }

  @Test
  public void testReleaseDownloaderCurrentMemoryUsage() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    final long initialMemoryUsage = SnowflakeChunkDownloader.getCurrentMemoryUsage();

    statement.executeQuery(
        "select current_date(), true,2345234, 2343.0, 'testrgint\\n\\t' from table(generator(rowcount=>200000))");

    assertThat("hold memory usage for the resultSet before close",
               SnowflakeChunkDownloader.getCurrentMemoryUsage() - initialMemoryUsage > 0);
    statement.close();
    assertThat("closing statement didn't release memory allocated for result",
               SnowflakeChunkDownloader.getCurrentMemoryUsage(), equalTo(initialMemoryUsage));
    connection.close();
  }

  @Test
  public void testDateTimeRelatedTypeConversion() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute(
        "create or replace table testDateTime" +
        "(colDate DATE, colTS timestamp_ltz, colTime TIME, colString string)");
    PreparedStatement preparedStatement = connection.prepareStatement(
        "insert into testDateTime values(?, ?, ?, ?)");

    Timestamp ts = buildTimestamp(2016, 3, 20, 3, 25, 45, 67800000);
    Date date = buildDate(2016, 3, 20);
    Time time = new Time(12345678); // 03:25:45.678

    preparedStatement.setDate(1, date);
    preparedStatement.setTimestamp(2, ts);
    preparedStatement.setTime(3, time);
    preparedStatement.setString(4, "aaa");

    preparedStatement.execute();
    ResultSet resultSet = statement.executeQuery("select * from testDateTime");
    resultSet.next();

    // ResultSet.getDate()
    assertEquals(date, resultSet.getDate("COLDATE"));
    try
    {
      resultSet.getDate("COLTIME");
      fail();
    }
    catch (SnowflakeSQLException e)
    {
      assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), e.getErrorCode());
      assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), e.getSQLState());
    }

    // ResultSet.getTimestamp()
    assertEquals(new Timestamp(date.getTime()), resultSet.getTimestamp("COLDATE"));
    assertEquals(ts, resultSet.getTimestamp("COLTS"));
    assertEquals(new Timestamp(time.getTime()), resultSet.getTimestamp("COLTIME"));
    try
    {
      resultSet.getTimestamp("COLSTRING");
      fail();
    }
    catch (SnowflakeSQLException e)
    {
      assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), e.getErrorCode());
      assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), e.getSQLState());
    }

    // ResultSet.getTime()
    try
    {
      resultSet.getTime("COLDATE");
      fail();
    }
    catch (SnowflakeSQLException e)
    {
      assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), e.getErrorCode());
      assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), e.getSQLState());
    }
    assertEquals(time, resultSet.getTime("COLTIME"));
    assertEquals(new Time(ts.getTime()), resultSet.getTime("COLTS"));

    statement.execute("drop table if exists testDateTime");
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testResultColumnSearchCaseSensitiveOld() throws Exception
  {
    subTestResultColumnSearchCaseSensitive("JDBC_RS_COLUMN_CASE_INSENSITIVE");
  }

  @Test
  public void testResultColumnSearchCaseSensitive() throws Exception
  {
    subTestResultColumnSearchCaseSensitive("CLIENT_RESULT_COLUMN_CASE_INSENSITIVE");
  }

  private void subTestResultColumnSearchCaseSensitive(String parameterName) throws Exception
  {
    Properties prop = new Properties();
    prop.put("tracing", "FINEST");
    Connection connection = getConnection(prop);
    Statement statement = connection.createStatement();

    ResultSet resultSet = statement.executeQuery("select 1 AS TESTCOL");

    resultSet.next();
    assertEquals("1", resultSet.getString("TESTCOL"));
    assertEquals("1", resultSet.getString("TESTCOL"));
    try
    {
      resultSet.getString("testcol");
      fail();
    }
    catch (SQLException e)
    {
      assertEquals("Column not found: testcol", e.getMessage());
    }

    // try to do case-insensitive search
    statement.executeQuery(
        String.format("alter session set %s=true", parameterName));

    resultSet = statement.executeQuery("select 1 AS TESTCOL");
    resultSet.next();

    // get twice so that the code path can hit the place where
    // we use cached key pair (columnName, index)
    assertEquals("1", resultSet.getString("TESTCOL"));
    assertEquals("1", resultSet.getString("TESTCOL"));
    assertEquals("1", resultSet.getString("testcol"));
    assertEquals("1", resultSet.getString("testcol"));
  }

  @Test
  public void testInvalidColumnIndex() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(selectAllSQL);

    resultSet.next();
    try
    {
      resultSet.getString(0);
      fail();
    }
    catch (SQLException e)
    {
      assertEquals(200032, e.getErrorCode());
    }
    try
    {
      resultSet.getString(2);
      fail();
    }
    catch (SQLException e)
    {
      assertEquals(200032, e.getErrorCode());
    }
    resultSet.close();
    statement.close();
    connection.close();
  }

  /**
   * SNOW-28882: wasNull was not set properly
   */
  @Test
  public void testWasNull() throws Exception
  {
    Connection con = getConnection();
    ResultSet ret = con.createStatement().executeQuery(
        "select cast(1/nullif(0,0) as double)," +
        "cast(1/nullif(0,0) as int), 100, " +
        "cast(1/nullif(0,0) as number(8,2))");
    ret.next();
    assertThat("Double value cannot be null",
               ret.getDouble(1), equalTo(0.0));
    assertThat("wasNull should be true", ret.wasNull());
    assertThat("Integer value cannot be null",
               ret.getInt(2), equalTo(0));
    assertThat("wasNull should be true", ret.wasNull());
    assertThat("Non null column",
               ret.getInt(3), equalTo(100));
    assertThat("wasNull should be false", !ret.wasNull());
    assertThat("BigDecimal value must be null",
               ret.getBigDecimal(4), nullValue());
    assertThat("wasNull should be true", ret.wasNull());
  }

  /**
   * SNOW-28390
   */
  @Test
  public void testParseInfAndNaNNumber() throws Exception
  {
    Connection con = getConnection();
    ResultSet ret = con.createStatement().executeQuery(
        "select to_double('inf'), to_double('-inf')");
    ret.next();
    assertThat("Positive Infinite Number",
               ret.getDouble(1), equalTo(Double.POSITIVE_INFINITY));
    assertThat("Negative Infinite Number",
               ret.getDouble(2), equalTo(Double.NEGATIVE_INFINITY));
    assertThat("Positive Infinite Number",
               ret.getFloat(1), equalTo(Float.POSITIVE_INFINITY));
    assertThat("Negative Infinite Number",
               ret.getFloat(2), equalTo(Float.NEGATIVE_INFINITY));

    ret = con.createStatement().executeQuery(
        "select to_double('nan')");
    ret.next();
    assertThat("Parse NaN",
               ret.getDouble(1), equalTo(Double.NaN));
    assertThat("Parse NaN",
               ret.getFloat(1), equalTo(Float.NaN));
  }

  /**
   * SNOW-33227
   */
  @Test
  public void testTreatDecimalAsInt() throws Exception
  {
    Connection con = getConnection();
    ResultSet ret = con.createStatement().executeQuery(
        "select 1");

    ResultSetMetaData metaData = ret.getMetaData();
    assertThat(metaData.getColumnType(1), equalTo(Types.BIGINT));

    con.createStatement().execute("alter session set jdbc_treat_decimal_as_int = false");

    ret = con.createStatement().executeQuery("select 1");
    metaData = ret.getMetaData();
    assertThat(metaData.getColumnType(1), equalTo(Types.DECIMAL));

    con.close();
  }

  @Test
  public void testIsLast() throws Exception
  {

    Connection con = getConnection();
    ResultSet ret = con.createStatement().executeQuery(
        "select * from orders_jdbc");
    assertTrue("should be before the first", ret.isBeforeFirst());
    assertFalse("should not be the first", ret.isFirst());

    ret.next();

    assertFalse("should not be before the first", ret.isBeforeFirst());
    assertTrue("should be the first", ret.isFirst());

    int cnt = 0;
    while (ret.next())
    {
      cnt++;
      if (cnt == 72)
      {
        assertTrue("should be the last", ret.isLast());
        assertFalse("should not be after the last", ret.isAfterLast());
      }
    }
    assertEquals(72, cnt);

    ret.next();

    assertFalse("should not be the last", ret.isLast());
    assertTrue("should be afterthe last", ret.isAfterLast());

    // PUT one file
    ret = con.createStatement().executeQuery(
        "PUT file://" +
        getFullPathFileInResource(TEST_DATA_FILE) + " @~");

    assertTrue("should be before the first", ret.isBeforeFirst());
    assertFalse("should not be the first", ret.isFirst());

    ret.next();

    assertFalse("should not be before the first", ret.isBeforeFirst());
    assertTrue("should be the first", ret.isFirst());

    assertTrue("should be the last", ret.isLast());
    assertFalse("should not be after the last", ret.isAfterLast());

    ret.next();

    assertFalse("should not be the last", ret.isLast());
    assertTrue("should be after the last", ret.isAfterLast());
  }

  @Test
  public void testGetOldTimestamp() throws SQLException
  {
    Connection con = getConnection();
    Statement statement = con.createStatement();

    statement.execute("create or replace table testOldTs(cola timestamp_ntz)");
    statement.execute("insert into testOldTs values ('1582-06-22 17:00:00'), " +
                      "('1000-01-01 17:00:00')");

    ResultSet resultSet = statement.executeQuery("select * from testOldTs");

    resultSet.next();

    assertThat(resultSet.getTimestamp(1).toString(), equalTo("1582-06-22 17:00:00.0"));
    assertThat(resultSet.getString(1), equalTo("Fri, 22 Jun 1582 17:00:00 Z"));

    resultSet.next();
    assertThat(resultSet.getTimestamp(1).toString(), equalTo("1000-01-01 17:00:00.0"));
    assertThat(resultSet.getString(1), equalTo("Mon, 01 Jan 1000 17:00:00 Z"));

    statement.execute("drop table if exists testOldTs");
    statement.close();
    con.close();
  }

  @Test
  public void testPrepareOldTimestamp() throws SQLException
  {
    Connection con = getConnection();
    Statement statement = con.createStatement();

    statement.execute("create or replace table testPrepOldTs(cola timestamp_ntz, colb date)");
    statement.execute("alter session set client_timestamp_type_mapping=timestamp_ntz");
    PreparedStatement ps = con.prepareStatement("insert into testPrepOldTs values (?, ?)");

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    ps.setTimestamp(1, Timestamp.valueOf("0001-01-01 08:00:00"));
    ps.setDate(2, java.sql.Date.valueOf("0001-01-01"));
    ps.executeUpdate();

    ResultSet resultSet = statement.executeQuery("select * from testPrepOldTs");

    resultSet.next();
    assertThat(resultSet.getTimestamp(1).toString(), equalTo("0001-01-01 08:00:00.0"));
    assertThat(resultSet.getDate(2).toString(), equalTo("0001-01-01"));

    statement.execute("drop table if exists testPrepOldTs");

    statement.close();

    con.close();
  }

  @Test
  public void testMultipleChunks() throws SQLException, IOException
  {
    Connection con = getConnection();
    Statement statement = con.createStatement();

    // 10000 rows should be enough to force result into multiple chunks
    ResultSet resultSet =
        statement.executeQuery("select seq8(), randstr(1000, random()) from table(generator(rowcount => 10000))");
    int cnt = 0;
    while (resultSet.next())
    {
      ++cnt;
    }
    assertTrue(cnt >= 0);
    Telemetry telemetry = con.unwrap(SnowflakeConnectionV1.class).getSfSession().getTelemetryClient();
    LinkedList<TelemetryData> logs = ((TelemetryClient) telemetry).logBuffer();

    // there should be a log for each of the following fields
    TelemetryField[] expectedFields =
        {TelemetryField.TIME_CONSUME_FIRST_RESULT, TelemetryField.TIME_CONSUME_LAST_RESULT,
         TelemetryField.TIME_WAITING_FOR_CHUNKS, TelemetryField.TIME_DOWNLOADING_CHUNKS,
         TelemetryField.TIME_PARSING_CHUNKS};
    boolean[] succeeded = new boolean[expectedFields.length];

    for (int i = 0; i < expectedFields.length; i++)
    {
      succeeded[i] = false;
      for (TelemetryData log : logs)
      {
        if (log.getMessage().get(TelemetryUtil.TYPE).textValue().equals(expectedFields[i].field))
        {
          succeeded[i] = true;
          break;
        }
      }
    }

    for (int i = 0; i < expectedFields.length; i++)
    {
      assertThat(String.format("%s field not found in telemetry logs\n", expectedFields[i].field), succeeded[i]);
    }
    telemetry.sendBatch();
  }

  @Test
  public void testUpdateCountOnCopyCmd() throws Exception
  {
    Connection con = getConnection();
    Statement statement = con.createStatement();

    statement.execute("create or replace table testcopy(cola string)");

    // stage table has no file. Should return 0.
    int rowCount = statement.executeUpdate("copy into testcopy");
    assertThat(rowCount, is(0));

    // copy one file into table stage
    statement.execute("copy into @%testcopy from (select 'test_string')");
    rowCount = statement.executeUpdate("copy into testcopy");
    assertThat(rowCount, is(1));

    //cleanup
    statement.execute("drop table if exists testcopy");

    con.close();
  }
}
