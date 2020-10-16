/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Test ResultSet */
@Category(TestCategoryResultSet.class)
public class ResultSetIT extends ResultSet0IT {
  private final String selectAllSQL = "select * from test_rs";

  private static final byte[] byteArrayTestCase1 = new byte[0];
  private static final byte[] byteArrayTestCase2 = {(byte) 0xAB, (byte) 0xCD, (byte) 0x12};
  private static final byte[] byteArrayTestCase3 = {
    (byte) 0x00, (byte) 0xFF, (byte) 0x42, (byte) 0x01
  };

  public ResultSetIT() {
    this("json");
  }

  ResultSetIT(String queryResultFormat) {
    super(queryResultFormat);
  }

  @Test
  public void testFindColumn() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(selectAllSQL);
    assertEquals(1, resultSet.findColumn("COLA"));
    statement.close();
    connection.close();
  }

  @Test
  public void testGetColumnClassNameForBinary() throws Throwable {
    Connection connection = init();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table bintable (b binary)");
    statement.execute("insert into bintable values ('00f1f2')");
    ResultSet resultSet = statement.executeQuery("select * from bintable");
    ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(SnowflakeType.BINARY_CLASS_NAME, metaData.getColumnClassName(1));
    assertTrue(resultSet.next());
    Class<?> klass = Class.forName(SnowflakeType.BINARY_CLASS_NAME);
    Object ret0 = resultSet.getObject(1);
    assertEquals(ret0.getClass(), klass);
    byte[] ret = (byte[]) ret0;
    assertEquals(3, ret.length);
    assertEquals(ret[0], (byte) 0);
    assertEquals(ret[1], (byte) -15);
    assertEquals(ret[2], (byte) -14);
    statement.execute("drop table if exists bintable");
    statement.close();
    connection.close();
  }

  @Test
  public void testGetMethod() throws Throwable {
    String prepInsertString = "insert into test_get values(?, ?, ?, ?, ?, ?, ?, ?)";
    int bigInt = Integer.MAX_VALUE;
    long bigLong = Long.MAX_VALUE;
    short bigShort = Short.MAX_VALUE;
    String str = "hello";
    double bigDouble = Double.MAX_VALUE;
    float bigFloat = Float.MAX_VALUE;

    Connection connection = init();
    Clob clob = connection.createClob();
    clob.setString(1, "hello world");
    Statement statement = connection.createStatement();
    statement.execute(
        "create or replace table test_get(colA integer, colB number, colC number, "
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

    // assertEquals(bigDouble, resultSet.getDouble(5), 0);
    // assertEquals(bigDouble, resultSet.getDouble("COLE"), 0);
    assertEquals(bigFloat, resultSet.getFloat(6), 0);
    assertEquals(bigFloat, resultSet.getFloat("COLF"), 0);
    assertTrue(resultSet.getBoolean(7));
    assertTrue(resultSet.getBoolean("COLG"));
    assertEquals("hello world", resultSet.getClob("COLH").toString());

    // test getStatement method
    assertEquals(statement, resultSet.getStatement());

    prepStatement.close();
    statement.execute("drop table if exists table_get");
    statement.close();
    resultSet.close();
    connection.close();
  }

  @Test
  public void testGetObjectOnDatabaseMetadataResultSet() throws SQLException {
    Connection connection = init();
    DatabaseMetaData databaseMetaData = connection.getMetaData();
    ResultSet resultSet = databaseMetaData.getTypeInfo();
    resultSet.next();
    // SNOW-21375 "NULLABLE" Column is a SMALLINT TYPE
    assertEquals(DatabaseMetaData.typeNullable, resultSet.getObject("NULLABLE"));
    resultSet.close();
    connection.close();
  }

  @Test
  public void testGetShort() throws SQLException {
    ResultSet resultSet = numberCrossTesting();
    resultSet.next();
    // assert that 0 is returned for null values for every type of value
    for (int i = 1; i < 13; i++) {
      assertEquals(0, resultSet.getShort(i));
    }

    resultSet.next();
    assertEquals(2, resultSet.getShort(1));
    assertEquals(5, resultSet.getShort(2));
    assertEquals(3, resultSet.getShort(3));
    assertEquals(1, resultSet.getShort(4));
    assertEquals(1, resultSet.getShort(5));
    assertEquals(1, resultSet.getShort(6));
    assertEquals(9126, resultSet.getShort(7));

    for (int i = 8; i < 13; i++) {
      try {
        resultSet.getShort(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
    resultSet.next();
    // certain column types can only have certain values when called by getShort() or else a
    // SQLexception is thrown.
    // These column types are varchar, char, and float.

    for (int i = 5; i < 7; i++) {
      try {
        resultSet.getShort(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
  }

  @Test
  public void testGetInt() throws SQLException {
    ResultSet resultSet = numberCrossTesting();
    resultSet.next();
    // assert that 0 is returned for null values for every type of value
    for (int i = 1; i < 13; i++) {
      assertEquals(0, resultSet.getInt(i));
    }

    resultSet.next();
    assertEquals(2, resultSet.getInt(1));
    assertEquals(5, resultSet.getInt(2));
    assertEquals(3, resultSet.getInt(3));
    assertEquals(1, resultSet.getInt(4));
    assertEquals(1, resultSet.getInt(5));
    assertEquals(1, resultSet.getInt(6));
    assertEquals(9126, resultSet.getInt(7));

    for (int i = 8; i < 13; i++) {
      try {
        resultSet.getInt(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
    resultSet.next();
    // certain column types can only have certain values when called by getInt() or else a
    // SQLException is thrown.
    // These column types are varchar, char, and float.
    for (int i = 5; i < 7; i++) {
      try {
        resultSet.getInt(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
  }

  @Test
  public void testGetLong() throws SQLException {
    ResultSet resultSet = numberCrossTesting();
    resultSet.next();
    // assert that 0 is returned for null values for every type of value
    for (int i = 1; i < 13; i++) {
      assertEquals(0, resultSet.getLong(i));
    }

    resultSet.next();
    assertEquals(2, resultSet.getLong(1));
    assertEquals(5, resultSet.getLong(2));
    assertEquals(3, resultSet.getLong(3));
    assertEquals(1, resultSet.getLong(4));
    assertEquals(1, resultSet.getLong(5));
    assertEquals(1, resultSet.getLong(6));
    assertEquals(9126, resultSet.getLong(7));

    for (int i = 8; i < 13; i++) {
      try {
        resultSet.getLong(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
    resultSet.next();
    // certain column types can only have certain values when called by getLong() or else a
    // SQLexception is thrown.
    // These column types are varchar, char, and float.
    for (int i = 5; i < 7; i++) {
      try {
        resultSet.getLong(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
  }

  @Test
  public void testGetFloat() throws SQLException {
    ResultSet resultSet = numberCrossTesting();
    resultSet.next();
    // assert that 0 is returned for null values for every type of value
    for (int i = 1; i < 13; i++) {
      assertEquals(0, resultSet.getFloat(i), .1);
    }

    resultSet.next();
    assertEquals(2, resultSet.getFloat(1), .1);
    assertEquals(5, resultSet.getFloat(2), .1);
    assertEquals(3.5, resultSet.getFloat(3), .1);
    assertEquals(1, resultSet.getFloat(4), .1);
    assertEquals(1, resultSet.getFloat(5), .1);
    assertEquals(1, resultSet.getFloat(6), .1);
    assertEquals(9126, resultSet.getFloat(7), .1);

    for (int i = 8; i < 13; i++) {
      try {
        resultSet.getFloat(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
    resultSet.next();
    // certain column types can only have certain values when called by getFloat() or else a
    // SQLexception is thrown.
    // These column types are varchar and char.
    for (int i = 5; i < 7; i++) {
      try {
        resultSet.getFloat(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
  }

  @Test
  public void testGetDouble() throws SQLException {
    ResultSet resultSet = numberCrossTesting();
    resultSet.next();
    // assert that 0 is returned for null values for every type of value
    for (int i = 1; i < 13; i++) {
      assertEquals(0, resultSet.getDouble(i), .1);
    }

    resultSet.next();
    assertEquals(2, resultSet.getDouble(1), .1);
    assertEquals(5, resultSet.getDouble(2), .1);
    assertEquals(3.5, resultSet.getDouble(3), .1);
    assertEquals(1, resultSet.getDouble(4), .1);
    assertEquals(1, resultSet.getDouble(5), .1);
    assertEquals(1, resultSet.getDouble(6), .1);
    assertEquals(9126, resultSet.getDouble(7), .1);

    for (int i = 8; i < 13; i++) {
      try {
        resultSet.getDouble(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
    resultSet.next();
    // certain column types can only have certain values when called by getDouble() or else a
    // SQLexception is thrown.
    // These column types are varchar and char.
    for (int i = 5; i < 7; i++) {
      try {
        resultSet.getDouble(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
  }

  @Test
  public void testGetBigDecimal() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_get(colA number(38,9))");
    PreparedStatement preparedStatement =
        connection.prepareStatement("insert into test_get values(?)");
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

    resultSet = numberCrossTesting();
    resultSet.next();
    for (int i = 1; i < 13; i++) {
      assertNull(resultSet.getBigDecimal(i));
    }
    resultSet.next();
    assertEquals(new BigDecimal(2), resultSet.getBigDecimal(1));
    assertEquals(new BigDecimal(5), resultSet.getBigDecimal(2));
    assertEquals(new BigDecimal(3.5), resultSet.getBigDecimal(3));
    assertEquals(new BigDecimal(1), resultSet.getBigDecimal(4));
    assertEquals(new BigDecimal(1), resultSet.getBigDecimal(5));
    assertEquals(new BigDecimal(1), resultSet.getBigDecimal(6));
    assertEquals(new BigDecimal(9126), resultSet.getBigDecimal(7));
    for (int i = 8; i < 13; i++) {
      try {
        resultSet.getBigDecimal(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
    resultSet.next();
    for (int i = 5; i < 7; i++) {
      try {
        resultSet.getBigDecimal(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }
  }

  @Test
  public void testCursorPosition() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    statement.execute(selectAllSQL);
    ResultSet resultSet = statement.getResultSet();
    resultSet.next();
    assertTrue(resultSet.isFirst());
    assertEquals(1, resultSet.getRow());
    resultSet.next();
    assertFalse(resultSet.isFirst());
    assertEquals(2, resultSet.getRow());
    assertFalse(resultSet.isLast());
    resultSet.next();
    assertEquals(3, resultSet.getRow());
    assertTrue(resultSet.isLast());
    resultSet.next();
    assertTrue(resultSet.isAfterLast());
    statement.close();
    connection.close();
  }

  /**
   * Gets bytes in HEX form.
   *
   * @throws SQLException arises if any exception occurs.
   */
  @Test
  public void testGetBytes() throws SQLException {
    Properties props = new Properties();
    Connection connection = init(props);
    ingestBinaryTestData(connection);

    // Get results in hex format (default).
    ResultSet resultSet = connection.createStatement().executeQuery("select * from bin");
    resultSet.next();
    assertArrayEquals(byteArrayTestCase1, resultSet.getBytes(1));
    assertEquals("", resultSet.getString(1));
    resultSet.next();
    assertArrayEquals(byteArrayTestCase2, resultSet.getBytes(1));
    assertEquals("ABCD12", resultSet.getString(1));
    resultSet.next();
    assertArrayEquals(byteArrayTestCase3, resultSet.getBytes(1));
    assertEquals("00FF4201", resultSet.getString(1));
    connection.createStatement().execute("drop table if exists bin");
    connection.close();
  }

  /**
   * Ingests the byte test data
   *
   * @param connection Connection
   * @throws SQLException arises if any exception occurs
   */
  private void ingestBinaryTestData(Connection connection) throws SQLException {
    connection.createStatement().execute("create or replace table bin (b Binary)");
    PreparedStatement prepStatement =
        connection.prepareStatement("insert into bin values (?), (?), (?)");
    prepStatement.setBytes(1, byteArrayTestCase1);
    prepStatement.setBytes(2, byteArrayTestCase2);
    prepStatement.setBytes(3, byteArrayTestCase3);
    prepStatement.execute();
  }

  /**
   * Get bytes in Base64
   *
   * @throws Exception arises if any error occurs
   */
  @Test
  public void testGetBytesInBase64() throws Exception {
    Properties props = new Properties();
    props.setProperty("binary_output_format", "BAse64");
    Connection connection = init(props);
    ingestBinaryTestData(connection);

    ResultSet resultSet = connection.createStatement().executeQuery("select * from bin");
    resultSet.next();
    assertArrayEquals(byteArrayTestCase1, resultSet.getBytes(1));
    assertEquals("", resultSet.getString(1));
    resultSet.next();
    assertArrayEquals(byteArrayTestCase2, resultSet.getBytes(1));
    assertEquals("q80S", resultSet.getString(1));
    resultSet.next();
    assertArrayEquals(byteArrayTestCase3, resultSet.getBytes(1));
    assertEquals("AP9CAQ==", resultSet.getString(1));

    connection.createStatement().execute("drop table if exists bin");
    connection.close();
  }

  // SNOW-31647
  @Test
  public void testColumnMetaWithZeroPrecision() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();

    statement.execute(
        "create or replace table testColDecimal(cola number(38, 0), " + "colb number(17, 5))");

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
  public void testGetObjectOnFixedView() throws Exception {
    Connection connection = init();
    Statement statement = connection.createStatement();

    statement.execute(
        "create or replace table testFixedView"
            + "(C1 STRING NOT NULL COMMENT 'JDBC', "
            + "C2 STRING, C3 STRING, C4 STRING, C5 STRING, C6 STRING, "
            + "C7 STRING, C8 STRING, C9 STRING) "
            + "stage_file_format = (field_delimiter='|' "
            + "error_on_column_count_mismatch=false)");

    // put files
    assertTrue(
        "Failed to put a file",
        statement.execute(
            "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @%testFixedView"));

    ResultSet resultSet =
        statement.executeQuery(
            "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE_2) + " @%testFixedView");

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    while (resultSet.next()) {
      for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
        assertNotNull(resultSet.getObject(i + 1));
      }
    }

    resultSet.close();
    statement.execute("drop table if exists testFixedView");
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetColumnDisplaySizeAndPrecision() throws SQLException {
    Connection connection = init();
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
    assertEquals(1, resultSetMetaData.getColumnDisplaySize(1));
    assertEquals(1, resultSetMetaData.getPrecision(1));

    resultSet = statement.executeQuery("select cast(1 as string(30))");
    resultSetMetaData = resultSet.getMetaData();
    assertEquals(1, resultSetMetaData.getColumnDisplaySize(1));
    assertEquals(1, resultSetMetaData.getPrecision(1));

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
  public void testGetBoolean() throws SQLException {
    Connection connection = init();
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

    statement.execute(
        "create or replace table test_types(c1 number, c2 integer,  c3 varchar, c4 char, "
            + "c5 boolean, c6 float, c7 binary, c8 date, c9 datetime, c10 time, c11 timestamp_ltz, "
            + "c12 timestamp_tz)");
    statement.execute(
        "insert into test_types values (null, null, null, null, null, null, null, null, null, null, "
            + "null, null)");
    statement.execute(
        "insert into test_types (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12) values(1, 1, '1',"
            + "'1', true, 1.0, '48454C4C4F', '1994-12-27', "
            + "'1994-12-27 05:05:05', '05:05:05', '1994-12-27 05:05:05 +00:05', '1994-12-27 05:05:05')");
    statement.execute("insert into test_types (c1, c2, c3, c4) values(2, 3, '4', '5')");
    resultSet = statement.executeQuery("select * from test_types");

    resultSet.next();
    // assert that getBoolean returns false for null values
    for (int i = 1; i < 13; i++) {
      assertFalse(resultSet.getBoolean(i));
    }
    // do the other columns that are out of order
    // go to next row of result set column
    resultSet.next();
    // assert that getBoolean returns true for values that equal 1
    assertTrue(resultSet.getBoolean(1));
    assertTrue(resultSet.getBoolean(2));
    assertTrue(resultSet.getBoolean(3));
    assertTrue(resultSet.getBoolean(4));
    assertTrue(resultSet.getBoolean(5));
    for (int i = 6; i < 13; i++) {
      try {
        resultSet.getBoolean(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }

    resultSet.next();
    for (int i = 1; i < 5; i++) {
      try {
        resultSet.getBoolean(i);
        fail("Failing on " + i);
      } catch (SQLException ex) {
        assertEquals(200038, ex.getErrorCode());
      }
    }

    statement.close();
    connection.close();
  }

  @Test
  public void testGetClob() throws Throwable {
    Connection connection = init();
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
  public void testFetchOnClosedResultSet() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(selectAllSQL);
    assertFalse(resultSet.isClosed());
    resultSet.close();
    assertTrue(resultSet.isClosed());
    assertFalse(resultSet.next());
  }

  @Test
  public void testReleaseDownloaderCurrentMemoryUsage() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    final long initialMemoryUsage = SnowflakeChunkDownloader.getCurrentMemoryUsage();

    statement.executeQuery(
        "select current_date(), true,2345234, 2343.0, 'testrgint\\n\\t' from table(generator(rowcount=>1000000))");

    assertThat(
        "hold memory usage for the resultSet before close",
        SnowflakeChunkDownloader.getCurrentMemoryUsage() - initialMemoryUsage >= 0);
    statement.close();
    assertThat(
        "closing statement didn't release memory allocated for result",
        SnowflakeChunkDownloader.getCurrentMemoryUsage(),
        equalTo(initialMemoryUsage));
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testResultColumnSearchCaseSensitiveOld() throws Exception {
    subTestResultColumnSearchCaseSensitive("JDBC_RS_COLUMN_CASE_INSENSITIVE");
  }

  @Test
  public void testResultColumnSearchCaseSensitive() throws Exception {
    subTestResultColumnSearchCaseSensitive("CLIENT_RESULT_COLUMN_CASE_INSENSITIVE");
  }

  private void subTestResultColumnSearchCaseSensitive(String parameterName) throws Exception {
    Properties prop = new Properties();
    prop.put("tracing", "FINEST");
    Connection connection = init(prop);
    Statement statement = connection.createStatement();

    ResultSet resultSet = statement.executeQuery("select 1 AS TESTCOL");

    resultSet.next();
    assertEquals("1", resultSet.getString("TESTCOL"));
    assertEquals("1", resultSet.getString("TESTCOL"));
    try {
      resultSet.getString("testcol");
      fail();
    } catch (SQLException e) {
      assertEquals("Column not found: testcol", e.getMessage());
    }

    // try to do case-insensitive search
    statement.executeQuery(String.format("alter session set %s=true", parameterName));

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
  public void testInvalidColumnIndex() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(selectAllSQL);

    resultSet.next();
    try {
      resultSet.getString(0);
      fail();
    } catch (SQLException e) {
      assertEquals(200032, e.getErrorCode());
    }
    try {
      resultSet.getString(2);
      fail();
    } catch (SQLException e) {
      assertEquals(200032, e.getErrorCode());
    }
    resultSet.close();
    statement.close();
    connection.close();
  }

  /** SNOW-28882: wasNull was not set properly */
  @Test
  public void testWasNull() throws Exception {
    Connection con = init();
    ResultSet ret =
        con.createStatement()
            .executeQuery(
                "select cast(1/nullif(0,0) as double),"
                    + "cast(1/nullif(0,0) as int), 100, "
                    + "cast(1/nullif(0,0) as number(8,2))");
    ret.next();
    assertThat("Double value cannot be null", ret.getDouble(1), equalTo(0.0));
    assertThat("wasNull should be true", ret.wasNull());
    assertThat("Integer value cannot be null", ret.getInt(2), equalTo(0));
    assertThat("wasNull should be true", ret.wasNull());
    assertThat("Non null column", ret.getInt(3), equalTo(100));
    assertThat("wasNull should be false", !ret.wasNull());
    assertThat("BigDecimal value must be null", ret.getBigDecimal(4), nullValue());
    assertThat("wasNull should be true", ret.wasNull());
  }

  /** SNOW-28390 */
  @Test
  public void testParseInfAndNaNNumber() throws Exception {
    Connection con = init();
    ResultSet ret =
        con.createStatement().executeQuery("select to_double('inf'), to_double('-inf')");
    ret.next();
    assertThat("Positive Infinite Number", ret.getDouble(1), equalTo(Double.POSITIVE_INFINITY));
    assertThat("Negative Infinite Number", ret.getDouble(2), equalTo(Double.NEGATIVE_INFINITY));
    assertThat("Positive Infinite Number", ret.getFloat(1), equalTo(Float.POSITIVE_INFINITY));
    assertThat("Negative Infinite Number", ret.getFloat(2), equalTo(Float.NEGATIVE_INFINITY));

    ret = con.createStatement().executeQuery("select to_double('nan')");
    ret.next();
    assertThat("Parse NaN", ret.getDouble(1), equalTo(Double.NaN));
    assertThat("Parse NaN", ret.getFloat(1), equalTo(Float.NaN));
  }

  /** SNOW-33227 */
  @Test
  public void testTreatDecimalAsInt() throws Exception {
    Connection con = init();
    ResultSet ret = con.createStatement().executeQuery("select 1");

    ResultSetMetaData metaData = ret.getMetaData();
    assertThat(metaData.getColumnType(1), equalTo(Types.BIGINT));

    con.createStatement().execute("alter session set jdbc_treat_decimal_as_int = false");

    ret = con.createStatement().executeQuery("select 1");
    metaData = ret.getMetaData();
    assertThat(metaData.getColumnType(1), equalTo(Types.DECIMAL));

    con.close();
  }

  @Test
  public void testIsLast() throws Exception {
    Connection con = init();
    ResultSet ret = con.createStatement().executeQuery("select * from orders_jdbc");
    assertTrue("should be before the first", ret.isBeforeFirst());
    assertFalse("should not be the first", ret.isFirst());

    ret.next();

    assertFalse("should not be before the first", ret.isBeforeFirst());
    assertTrue("should be the first", ret.isFirst());

    int cnt = 0;
    while (ret.next()) {
      cnt++;
      if (cnt == 72) {
        assertTrue("should be the last", ret.isLast());
        assertFalse("should not be after the last", ret.isAfterLast());
      }
    }
    assertEquals(72, cnt);

    ret.next();

    assertFalse("should not be the last", ret.isLast());
    assertTrue("should be afterthe last", ret.isAfterLast());

    // PUT one file
    ret =
        con.createStatement()
            .executeQuery("PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @~");

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
  public void testUpdateCountOnCopyCmd() throws Exception {
    Connection con = init();
    Statement statement = con.createStatement();

    statement.execute("create or replace table testcopy(cola string)");

    // stage table has no file. Should return 0.
    int rowCount = statement.executeUpdate("copy into testcopy");
    assertThat(rowCount, is(0));

    // copy one file into table stage
    statement.execute("copy into @%testcopy from (select 'test_string')");
    rowCount = statement.executeUpdate("copy into testcopy");
    assertThat(rowCount, is(1));

    // cleanup
    statement.execute("drop table if exists testcopy");

    con.close();
  }

  @Test
  public void testGetTimeNullTimestampAndTimestampNullTime() throws Throwable {
    try (Connection con = init()) {
      con.createStatement().execute("create or replace table testnullts(c1 timestamp, c2 time)");
      try {
        con.createStatement().execute("insert into testnullts(c1, c2) values(null, null)");
        ResultSet rs = con.createStatement().executeQuery("select * from testnullts");
        assertTrue("should return result", rs.next());
        assertNull("return value must be null", rs.getTime(1));
        assertNull("return value must be null", rs.getTimestamp(2));
        rs.close();
      } finally {
        con.createStatement().execute("drop table if exists testnullts");
      }
    }
  }
}
