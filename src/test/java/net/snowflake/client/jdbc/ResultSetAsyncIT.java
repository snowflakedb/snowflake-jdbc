/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test AsyncResultSet
 */
@Category(TestCategoryResultSet.class)
public class ResultSetAsyncIT extends BaseJDBCTest
{
  @Test
  public void testAsyncResultSetFunctionsWithNewSession() throws SQLException
  {
    Connection connection = getConnection();
    final Map<String, String> params = getConnectionParameters();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
    statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
    String createTableSql = "select * from test_rsmd";
    ResultSet
        rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(createTableSql);
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
    assertEquals("",
                 resultSetMetaData.getCatalogName(1).toUpperCase());
    assertEquals("",
                 resultSetMetaData.getSchemaName(1).toUpperCase());
    assertEquals("", resultSetMetaData.getTableName(1));
    assertEquals(String.class.getName(),
                 resultSetMetaData.getColumnClassName(2));
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
    assertEquals(ResultSetMetaData.columnNullable,
                 resultSetMetaData.isNullable(1));
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
    assertTrue(Pattern
                   .matches(
                       "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
                       secretMetaData.getQueryID()));
    assertEquals(secretMetaData.getQueryID(), resultSet.unwrap(SnowflakeResultSet.class).getQueryID());
    resultSet.close();
    statement.close();
    connection.close();
  }

  @Test
  public void testResultSetMetadata() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
    statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
    ResultSet resultSet = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from test_rsmd");
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertEquals("",
                 resultSetMetaData.getCatalogName(1).toUpperCase());
    assertEquals("",
                 resultSetMetaData.getSchemaName(1).toUpperCase());
    assertEquals("", resultSetMetaData.getTableName(1));
    assertEquals(String.class.getName(),
                 resultSetMetaData.getColumnClassName(2));
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
    assertEquals(ResultSetMetaData.columnNullable,
                 resultSetMetaData.isNullable(1));
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
    assertTrue(Pattern
                   .matches("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
                            secretMetaData.getQueryID()));
    assertEquals(secretMetaData.getQueryID(), resultSet.unwrap(SnowflakeResultSet.class).getQueryID());

    statement.execute("drop table if exists test_rsmd");
    statement.close();
    connection.close();
  }

  @Test
  public void testisClosedIsLastisAfterLast() throws SQLException
  {
    // Set up environment
    Connection connection = getConnection();
    final Map<String, String> params = getConnectionParameters();
    Statement statement = connection.createStatement();
    statement.execute(
        "create or replace table test_rsmd(colA number(20, 5), colB string)");
    statement
        .execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
    ResultSet resultSet = statement.unwrap(SnowflakeStatement.class)
        .executeAsyncQuery("select * from test_rsmd");
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
    resultSet =
        connection.unwrap(SnowflakeConnection.class).createResultSet(queryID);
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
}
