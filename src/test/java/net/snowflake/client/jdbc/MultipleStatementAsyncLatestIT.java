/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.Random;
import net.snowflake.client.category.TestCategoryStatement;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Multi Statement tests */
@Category(TestCategoryStatement.class)
public class MultipleStatementAsyncLatestIT extends BaseJDBCTest {
  protected static String queryResultFormat = "json";
  protected static Random random = new Random();

  public static Connection getConnection() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection();
    Statement stmt = conn.createStatement();
    stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    stmt.close();
    return conn;
  }

  @Test
  public void testMultiStmtAsyncExecuteQuery() throws Throwable {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    String tableName = "TEST_TABLE_" + Math.abs(random.nextInt());
    try {
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
      String multiStmtQuery =
          "create or replace temporary table "
              + tableName
              + " (cola int);\n"
              + "insert into "
              + tableName
              + " VALUES (1), (2);\n"
              + "select cola from "
              + tableName
              + " order by cola asc";

      ResultSet mrs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(multiStmtQuery);
      // first statement always return a ResultSet
      ResultSet rs = statement.getResultSet();
      assertTrue(mrs == rs);
      rs.next();
      assertEquals("Table " + tableName + " successfully created.", rs.getString(1));
      assertEquals(-1, statement.getUpdateCount());

      // second statement
      assertFalse(statement.getMoreResults());
      assertNull(statement.getResultSet());
      assertEquals(2, statement.getUpdateCount());

      // third statement
      assertTrue(statement.getMoreResults());
      assertEquals(-1, statement.getUpdateCount());
      rs = statement.getResultSet();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertFalse(rs.next());

      assertFalse(statement.getMoreResults());
      assertEquals(-1, statement.getUpdateCount());
    } finally {
      connection.createStatement().execute("drop table if exists " + tableName);
      statement.close();
      connection.close();
    }
  }

  @Test
  public void testMultiStmtAsyncInOtherStatement() throws Throwable {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    String tableName = "TEST_TABLE_" + Math.abs(random.nextInt());
    try {
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
      String multiStmtQuery =
          "create or replace temporary table "
              + tableName
              + " (cola int);\n"
              + "insert into "
              + tableName
              + " VALUES (1), (2);\n"
              + "select cola from "
              + tableName
              + " order by cola asc";

      ResultSet mrs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(multiStmtQuery);
      String queryId = mrs.unwrap(SnowflakeResultSet.class).getQueryID();

      // Get Statement for multiple statement
      Statement otherStatement =
          connection
              .unwrap(SnowflakeConnection.class)
              .createStatementForMultipleStatementsQuery(queryId);

      // first statement must return a result set.
      ResultSet rs = otherStatement.getResultSet();
      rs.next();
      assertEquals("Table " + tableName + " successfully created.", rs.getString(1));
      assertEquals(-1, otherStatement.getUpdateCount());

      // second statement.
      assertFalse(otherStatement.getMoreResults());
      assertNull(otherStatement.getResultSet());
      assertEquals(2, otherStatement.getUpdateCount());

      // third statement.
      assertTrue(otherStatement.getMoreResults());
      assertEquals(-1, otherStatement.getUpdateCount());
      rs = otherStatement.getResultSet();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertFalse(rs.next());

      assertFalse(otherStatement.getMoreResults());
      assertEquals(-1, otherStatement.getUpdateCount());
    } finally {
      connection.createStatement().execute("drop table if exists " + tableName);
      statement.close();
      connection.close();
    }
  }

  @Test
  public void testMultiStmtAsyncInOtherStatementCRT() throws Throwable {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    String tableName = "TEST_TABLE_" + Math.abs(random.nextInt());
    String tableName2 = "TEST_TABLE_" + Math.abs(random.nextInt());
    try {
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 4);
      String multiStmtQuery =
          "create or replace temporary table "
              + tableName
              + " (cola int);\n"
              + "insert into "
              + tableName
              + " VALUES (1), (2);\n"
              + "select cola from "
              + tableName
              + " order by cola asc;\n"
              + "create or replace temporary table "
              + tableName2
              + " AS "
              + "select cola from "
              + tableName
              + " where cola  = 1";

      ResultSet mrs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(multiStmtQuery);
      String queryId = mrs.unwrap(SnowflakeResultSet.class).getQueryID();

      // Get Statement for multiple statement
      Statement otherStatement =
          connection
              .unwrap(SnowflakeConnection.class)
              .createStatementForMultipleStatementsQuery(queryId);

      // first statement
      ResultSet rs = otherStatement.getResultSet();
      rs.next();
      assertEquals("Table " + tableName + " successfully created.", rs.getString(1));
      assertEquals(-1, otherStatement.getUpdateCount());

      // second statement
      assertFalse(otherStatement.getMoreResults());
      assertNull(otherStatement.getResultSet());
      assertEquals(2, otherStatement.getUpdateCount());

      // third statement
      assertTrue(otherStatement.getMoreResults());
      assertEquals(-1, otherStatement.getUpdateCount());
      rs = otherStatement.getResultSet();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertFalse(rs.next());

      // fourth statement: CREATE TABLE
      assertFalse(otherStatement.getMoreResults());
      assertEquals(-1, otherStatement.getUpdateCount());
    } finally {
      connection.createStatement().execute("drop table if exists " + tableName);
      connection.createStatement().execute("drop table if exists " + tableName2);
      statement.close();
      connection.close();
    }
  }

  @Test
  public void testMultiStmtAsyncExecuteFailedQueryInNewStatement() throws Throwable {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    String tableName = "TEST_TABLE_" + Math.abs(random.nextInt());
    try {
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      // second statement will fail.
      String multiStmtQuery =
          "create or replace temporary table "
              + tableName
              + " (cola int);\n"
              + "insert into "
              + tableName
              + " VALUES (1), (2, 2);";

      ResultSet mrs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(multiStmtQuery);
      String queryId = mrs.unwrap(SnowflakeResultSet.class).getQueryID();
      try {
        connection
            .unwrap(SnowflakeConnection.class)
            .createStatementForMultipleStatementsQuery(queryId);
        fail();
      } catch (Exception e) {
        assertTrue(
            e.getMessage()
                .contains("Status of query associated with resultSet is FAILED_WITH_ERROR."));
      }
    } finally {
      connection.createStatement().execute("drop table if exists " + tableName);
      statement.close();
      connection.close();
    }
  }

  @Test
  public void testMultiStmtAsyncExecuteFailedQuery() throws Throwable {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    String tableName = "TEST_TABLE_" + Math.abs(random.nextInt());
    try {
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      // second statement will fail.
      String multiStmtQuery =
          "create or replace temporary table "
              + tableName
              + " (cola int);\n"
              + "insert into "
              + tableName
              + " VALUES (1), (2, 2);";

      ResultSet mrs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(multiStmtQuery);
      String queryId = mrs.unwrap(SnowflakeResultSet.class).getQueryID();
      try {
        mrs.next();
        fail();
      } catch (Exception e) {
        assertTrue(
            e.getMessage()
                .contains("Status of query associated with resultSet is FAILED_WITH_ERROR."));
      }
    } finally {
      connection.createStatement().execute("drop table if exists " + tableName);
      statement.close();
      connection.close();
    }
  }
}
