/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.*;
import java.util.List;
import net.snowflake.client.category.TestCategoryStatement;
import net.snowflake.common.core.SqlState;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** Statement tests */
@Category(TestCategoryStatement.class)
public class StatementIT extends BaseJDBCTest {
  protected static String queryResultFormat = "json";

  public static Connection getConnection() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection();
    Statement stmt = conn.createStatement();
    stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    stmt.close();
    return conn;
  }

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Ignore("Not working for setFetchSize")
  @Test
  public void testFetchSize() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    assertEquals(50, statement.getFetchSize());
    statement.setFetchSize(1);
    ResultSet rs = statement.executeQuery("select * from JDBC_STATEMENT");
    assertEquals(1, getSizeOfResultSet(rs));

    statement.close();
    connection.close();
  }

  @Test
  public void testExecuteUpdateAndDelete() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    statement.execute(
        "create or replace table test_update(cola number, colb string) " + "as select 1, 'str1'");

    statement.execute("insert into test_update values(2, 'str2')");

    int updateCount;
    boolean success;
    updateCount = statement.executeUpdate("update test_update set COLB = 'newStr' where COLA = 1");
    assertEquals(1, updateCount);

    success = statement.execute("update test_update set COLB = 'newStr' where COLA = 2");
    assertFalse(success);
    assertEquals(1, statement.getUpdateCount());
    assertEquals(1L, statement.getLargeUpdateCount());
    assertNull(statement.getResultSet());

    updateCount = statement.executeUpdate("delete from test_update where colA = 1");
    assertEquals(1, updateCount);

    success = statement.execute("delete from test_update where colA = 2");
    assertFalse(success);
    assertEquals(1, statement.getUpdateCount());
    assertEquals(1L, statement.getLargeUpdateCount());
    assertNull(statement.getResultSet());

    statement.execute("drop table if exists test_update");
    statement.close();

    connection.close();
  }

  @Test
  public void testExecuteBatch() throws Exception {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    connection.setAutoCommit(false);
    // mixed of ddl/dml in batch
    statement.addBatch("create or replace table test_batch(a string, b integer)");
    statement.addBatch("insert into test_batch values('str1', 1), ('str2', 2)");
    statement.addBatch(
        "update test_batch set test_batch.b = src.b + 5 from "
            + "(select 'str1' as a, 2 as b) src where test_batch.a = src.a");

    int[] rowCounts = statement.executeBatch();
    connection.commit();

    assertThat(rowCounts.length, is(3));
    assertThat(rowCounts[0], is(0));
    assertThat(rowCounts[1], is(2));
    assertThat(rowCounts[2], is(1));

    List<String> batchQueryIDs = statement.unwrap(SnowflakeStatement.class).getBatchQueryIDs();
    assertEquals(3, batchQueryIDs.size());
    assertEquals(statement.unwrap(SnowflakeStatement.class).getQueryID(), batchQueryIDs.get(2));

    ResultSet resultSet = statement.executeQuery("select * from test_batch order by b asc");
    resultSet.next();
    assertThat(resultSet.getInt("B"), is(2));
    resultSet.next();
    assertThat(resultSet.getInt("B"), is(7));
    statement.clearBatch();

    // one of the batch is query instead of ddl/dml
    // it should continuing processing
    try {
      statement.addBatch("insert into test_batch values('str3', 3)");
      statement.addBatch("select * from test_batch");
      statement.addBatch("select * from test_batch_not_exist");
      statement.addBatch("insert into test_batch values('str4', 4)");
      statement.executeBatch();
      fail();
    } catch (BatchUpdateException e) {
      rowCounts = e.getUpdateCounts();
      assertThat(e.getErrorCode(), is(ERROR_CODE_DOMAIN_OBJECT_DOES_NOT_EXIST));
      assertThat(rowCounts[0], is(1));
      assertThat(rowCounts[1], is(Statement.SUCCESS_NO_INFO));
      assertThat(rowCounts[2], is(Statement.EXECUTE_FAILED));
      assertThat(rowCounts[3], is(1));

      connection.rollback();
    }

    statement.clearBatch();

    statement.addBatch(
        "put file://"
            + getFullPathFileInResource(TEST_DATA_FILE)
            + " @%test_batch auto_compress=false");
    File tempFolder = tmpFolder.newFolder("test_downloads_folder");
    statement.addBatch("get @%test_batch file://" + tempFolder);

    rowCounts = statement.executeBatch();
    assertThat(rowCounts.length, is(2));
    assertThat(rowCounts[0], is(Statement.SUCCESS_NO_INFO));
    assertThat(rowCounts[0], is(Statement.SUCCESS_NO_INFO));
    statement.clearBatch();

    statement.execute("drop table if exists test_batch");
    statement.close();
    connection.close();
  }

  @Test
  public void testMultiStmtNotEnabled() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String multiStmtQuery =
        "create or replace temporary table test_multi (cola int);\n"
            + "insert into test_multi VALUES (1), (2);\n"
            + "select cola from test_multi order by cola asc";

    try {
      statement.execute(multiStmtQuery);
      fail("Using a multi-statement query without the parameter set should fail");
    } catch (SnowflakeSQLException ex) {
      assertEquals(SqlState.FEATURE_NOT_SUPPORTED, ex.getSQLState());
    }

    statement.close();
    connection.close();
  }

  @Test
  public void testCreateStatementWithParameters() throws Throwable {
    try (Connection connection = getConnection()) {
      connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      try {
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        fail("updateable cursor is not supported.");
      } catch (SQLException ex) {
        assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());
      }
      connection.createStatement(
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY,
          ResultSet.CLOSE_CURSORS_AT_COMMIT);
      try {
        connection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
        fail("hold cursor over commit is not supported.");
      } catch (SQLException ex) {
        assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());
      }
    }
  }
}
