/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

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
    assertTrue(
        Pattern.matches(
            "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
            secretMetaData.getQueryID()));
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
    assertTrue(
        Pattern.matches(
            "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
            secretMetaData.getQueryID()));
    assertEquals(
        secretMetaData.getQueryID(), resultSet.unwrap(SnowflakeResultSet.class).getQueryID());

    statement.execute("drop table if exists test_rsmd");
    statement.close();
    connection.close();
  }

  @Test
  public void testisClosedIsLastisAfterLast() throws SQLException {
    // Set up environment
    Connection connection = getConnection();
    final Map<String, String> params = getConnectionParameters();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
    statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
    ResultSet resultSet =
        statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from test_rsmd");
    String queryID = resultSet.unwrap(SnowflakeResultSet.class).getQueryID();
    // test isClosed functions
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
  public void testGetMethods() throws Throwable {
    String prepInsertString = "insert into test_get values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
    statement.execute("create or replace table test_get(colA integer, colB number, colC number, colD string, colE double, colF float, colG boolean, colH text, colI binary, colJ number(38,9), colK int, colL date, colM time, colN timestamp_ltz)");

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
    resultSet.getMetaData();
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

    //assertEquals(bytes, resultSet.getBytes(9));
    //assertEquals(bytes, resultSet.getBytes("COLI"));
    //assertEquals(bigDecimal, resultSet.getBigDecimal(10));
    //assertEquals(bigDecimal, resultSet.getBigDecimal("COLJ"));
    assertEquals(oneByte, resultSet.getByte(11));
    assertEquals(oneByte, resultSet.getByte("COLK"));


    assertEquals(date, resultSet.getDate(12));
    assertEquals(date, resultSet.getDate("COLL"));
    assertEquals(time, resultSet.getTime(13));
    assertEquals(time, resultSet.getTime("COLM"));
    assertEquals(ts, resultSet.getTimestamp(14));
    assertEquals(ts, resultSet.getTimestamp("COLN"));

    // test getObject
    assertEquals(str, resultSet.getObject(4).toString());


    // test getStatement method
    assertEquals(statement, resultSet.getStatement());

    prepStatement.close();
    statement.execute("drop table if exists table_get");
    statement.close();
    resultSet.close();
    connection.close();
  }
}
