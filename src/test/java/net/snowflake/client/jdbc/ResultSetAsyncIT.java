/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import static org.junit.Assert.*;

import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import net.snowflake.client.TestUtil;
import net.snowflake.client.category.TestCategoryResultSet;
import net.snowflake.common.core.SqlState;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Test AsyncResultSet */
@Category(TestCategoryResultSet.class)
public class ResultSetAsyncIT extends BaseJDBCTest {
  @Test
  public void testAsyncResultSetFunctionsWithNewSession() throws SQLException {
    Connection connection = getConnection();
    final Map<String, String> params = getConnectionParameters();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
    statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
    String createTableSql = "select * from test_rsmd";
    ResultSet rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(createTableSql);
    String queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
    statement.execute("drop table if exists test_rsmd");
    rs.close();
    // close statement and connection
    statement.close();
    connection.close();
    connection = getConnection();
    // open a new connection and create a result set
    ResultSet resultSet = connection.unwrap(SnowflakeConnection.class).createResultSet(queryID);
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    // getCatalogName(), getSchemaName(), and getTableName() are empty
    // when session is re-opened
    assertEquals("", resultSetMetaData.getCatalogName(1).toUpperCase());
    assertEquals("", resultSetMetaData.getSchemaName(1).toUpperCase());
    assertEquals("", resultSetMetaData.getTableName(1));
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
    assertEquals(
        secretMetaData.getQueryID(), resultSet.unwrap(SnowflakeResultSet.class).getQueryID());
    resultSet.close();
    statement.close();
    connection.close();
  }

  @Test
  public void testResultSetMetadata() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
    statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
    ResultSet resultSet =
        statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from test_rsmd");
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertEquals("", resultSetMetaData.getCatalogName(1).toUpperCase());
    assertEquals("", resultSetMetaData.getSchemaName(1).toUpperCase());
    assertEquals("", resultSetMetaData.getTableName(1));
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
    assertEquals(
        secretMetaData.getQueryID(), resultSet.unwrap(SnowflakeResultSet.class).getQueryID());

    statement.execute("drop table if exists test_rsmd");
    statement.close();
    connection.close();
  }

  @Test
  public void testOrderAndClosureFunctions() throws SQLException {
    // Set up environment
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
    statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
    ResultSet resultSet =
        statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from test_rsmd");

    // test isFirst, isBeforeFirst
    assertTrue("should be before the first", resultSet.isBeforeFirst());
    assertFalse("should not be the first", resultSet.isFirst());
    resultSet.next();
    assertFalse("should not be before the first", resultSet.isBeforeFirst());
    assertTrue("should be the first", resultSet.isFirst());

    // test isClosed functions
    String queryID = resultSet.unwrap(SnowflakeResultSet.class).getQueryID();
    assertFalse(resultSet.isClosed());
    // close resultSet and test again
    resultSet.close();
    assertTrue(resultSet.isClosed());
    // close connection and open a new one
    statement.execute("drop table if exists test_rsmd");
    statement.close();
    connection.close();
    connection = getConnection();
    resultSet = connection.unwrap(SnowflakeConnection.class).createResultSet(queryID);
    // test out isClosed, isLast, and isAfterLast
    assertFalse(resultSet.isClosed());
    resultSet.next();
    resultSet.next();
    // cursor should be on last row
    assertTrue(resultSet.isLast());
    resultSet.next();
    // cursor is after last row
    assertTrue(resultSet.isAfterLast());
    resultSet.close();
    // resultSet should be closed
    assertTrue(resultSet.isClosed());
    statement.close();
    connection.close();
  }

  @Test
  public void testWasNull() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    Clob emptyClob = connection.createClob();
    emptyClob.setString(1, "");
    statement.execute(
        "create or replace table test_null(colA number, colB string, colNull string, emptyClob string)");
    PreparedStatement prepst =
        connection.prepareStatement("insert into test_null values (?, ?, ?, ?)");
    prepst.setNull(1, Types.INTEGER);
    prepst.setString(2, "hello");
    prepst.setString(3, null);
    prepst.setClob(4, emptyClob);
    prepst.execute();

    ResultSet resultSet =
        statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from test_null");
    resultSet.next();
    resultSet.getInt(1);
    assertTrue(resultSet.wasNull()); // integer value is null
    resultSet.getString(2);
    assertFalse(resultSet.wasNull()); // string value is not null
    assertNull(resultSet.getClob(3));
    assertNull(resultSet.getClob("COLNULL"));
    assertEquals("", resultSet.getClob("EMPTYCLOB").toString());
  }

  @Test
  public void testGetMethods() throws Throwable {
    String prepInsertString =
        "insert into test_get values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    int bigInt = Integer.MAX_VALUE;
    long bigLong = Long.MAX_VALUE;
    short bigShort = Short.MAX_VALUE;
    String str = "hello";
    double bigDouble = Double.MAX_VALUE;
    float bigFloat = Float.MAX_VALUE;
    byte[] bytes = {(byte) 0xAB, (byte) 0xCD, (byte) 0x12};
    BigDecimal bigDecimal = new BigDecimal("10000000000");
    byte oneByte = (byte) 1;
    Date date = new Date(44);
    Time time = new Time(500);
    Timestamp ts = new Timestamp(333);

    Connection connection = getConnection();
    Clob clob = connection.createClob();
    clob.setString(1, "hello world");
    Statement statement = connection.createStatement();
    statement.execute(
        "create or replace table test_get(colA integer, colB number, colC number, colD string, colE double, colF float, colG boolean, colH text, colI binary(3), colJ number(38,9), colK int, colL date, colM time, colN timestamp_ltz)");

    PreparedStatement prepStatement = connection.prepareStatement(prepInsertString);
    prepStatement.setInt(1, bigInt);
    prepStatement.setLong(2, bigLong);
    prepStatement.setLong(3, bigShort);
    prepStatement.setString(4, str);
    prepStatement.setDouble(5, bigDouble);
    prepStatement.setFloat(6, bigFloat);
    prepStatement.setBoolean(7, true);
    prepStatement.setClob(8, clob);
    prepStatement.setBytes(9, bytes);
    prepStatement.setBigDecimal(10, bigDecimal);
    prepStatement.setByte(11, oneByte);
    prepStatement.setDate(12, date);
    prepStatement.setTime(13, time);
    prepStatement.setTimestamp(14, ts);
    prepStatement.execute();

    ResultSet resultSet =
        statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from test_get");
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

    assertEquals(bigDouble, resultSet.getDouble(5), 0);
    assertEquals(bigDouble, resultSet.getDouble("COLE"), 0);
    assertEquals(bigFloat, resultSet.getFloat(6), 0);
    assertEquals(bigFloat, resultSet.getFloat("COLF"), 0);
    assertTrue(resultSet.getBoolean(7));
    assertTrue(resultSet.getBoolean("COLG"));
    assertEquals("hello world", resultSet.getClob("COLH").toString());

    // TODO: figure out why getBytes returns an offset.
    // assertEquals(bytes, resultSet.getBytes(9));
    // assertEquals(bytes, resultSet.getBytes("COLI"));

    DecimalFormat df = new DecimalFormat("#.00");
    assertEquals(df.format(bigDecimal), df.format(resultSet.getBigDecimal(10)));
    assertEquals(df.format(bigDecimal), df.format(resultSet.getBigDecimal("COLJ")));

    assertEquals(oneByte, resultSet.getByte(11));
    assertEquals(oneByte, resultSet.getByte("COLK"));

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    assertEquals(sdf.format(date), sdf.format(resultSet.getDate(12)));
    assertEquals(sdf.format(date), sdf.format(resultSet.getDate("COLL")));
    assertEquals(time, resultSet.getTime(13));
    assertEquals(time, resultSet.getTime("COLM"));
    assertEquals(ts, resultSet.getTimestamp(14));
    assertEquals(ts, resultSet.getTimestamp("COLN"));

    // test getObject
    assertEquals(str, resultSet.getObject(4).toString());
    assertEquals(str, resultSet.getObject("COLD").toString());

    // test getStatement method
    assertEquals(statement, resultSet.getStatement());

    prepStatement.close();
    statement.execute("drop table if exists table_get");
    statement.close();
    resultSet.close();
    connection.close();
  }

  /**
   * This is a corner case for if a user forgets to call one of these functions before attempting to
   * fetch real data. An empty ResultSet is initially returned form executeAsyncQuery() but is
   * replaced by a real ResultSet when next() or getMetaData() is called. If neither is called, user
   * can try to get results from empty result set but exceptions are thrown whenever a column name
   * cannot be found.
   *
   * @throws SQLException
   */
  @Test
  public void testEmptyResultSet() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    ResultSet rs =
        statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from empty_table");
    // if user never calls getMetadata() or next(), empty result set is used to get results.
    // empty ResultSet returns all nulls, 0s, and empty values.
    assertFalse(rs.isClosed());
    assertEquals(0, rs.getInt(1));
    try {
      rs.getInt("col1");
      fail("Fetching from a column name that does not exist should return a SQLException");
    } catch (SQLException e) {
      // findColumn fails with empty metadata with exception "Column not found".
      assertEquals(SqlState.UNDEFINED_COLUMN, e.getSQLState());
    }
    rs.close(); // close empty result set
    assertTrue(rs.isClosed());
    connection.close();
  }
}
