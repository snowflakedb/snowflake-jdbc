/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.*;
import java.util.List;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryStatement;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
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

  @Test
  public void testFetchDirection() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
    try {
      statement.setFetchDirection(ResultSet.FETCH_REVERSE);
    } catch (SQLFeatureNotSupportedException e) {
      assertTrue(true);
    }
    statement.close();
    connection.close();
  }

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
  public void testMaxRows() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String sqlSelect = "select seq4() from table(generator(rowcount=>3))";
    assertEquals(0, statement.getMaxRows());

    //    statement.setMaxRows(1);
    //    assertEquals(1, statement.getMaxRows());
    ResultSet rs = statement.executeQuery(sqlSelect);
    int resultSizeCount = getSizeOfResultSet(rs);
    //    assertEquals(1, resultSizeCount);

    statement.setMaxRows(0);
    rs = statement.executeQuery(sqlSelect);
    //    assertEquals(3, getSizeOfResultSet(rs));

    statement.setMaxRows(-1);
    rs = statement.executeQuery(sqlSelect);
    //    assertEquals(3, getSizeOfResultSet(rs));
    statement.close();

    connection.close();
  }

  @Test
  public void testQueryTimeOut() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    assertEquals(0, statement.getQueryTimeout());
    statement.setQueryTimeout(5);
    assertEquals(5, statement.getQueryTimeout());
    try {
      statement.executeQuery("select count(*) from table(generator(timeLimit => 100))");
    } catch (SQLException e) {
      assertTrue(true);
      assertEquals(SqlState.QUERY_CANCELED, e.getSQLState());
      assertEquals("SQL execution canceled", e.getMessage());
    }
    statement.close();
    connection.close();
  }

  @Test
  public void testStatementClose() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    assertEquals(connection, statement.getConnection());
    assertTrue(!statement.isClosed());
    statement.close();
    assertTrue(statement.isClosed());
    connection.close();
  }

  @Test
  public void testExecuteSelect() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    String sqlSelect = "select seq4() from table(generator(rowcount=>3))";
    boolean success = statement.execute(sqlSelect);
    assertTrue(success);
    String queryID1 = statement.unwrap(SnowflakeStatement.class).getQueryID();
    assertNotNull(queryID1);

    ResultSet rs = statement.getResultSet();
    assertEquals(3, getSizeOfResultSet(rs));
    assertEquals(-1, statement.getUpdateCount());
    assertEquals(-1L, statement.getLargeUpdateCount());
    String queryID2 = rs.unwrap(SnowflakeResultSet.class).getQueryID();
    assertEquals(queryID2, queryID1);

    rs = statement.executeQuery(sqlSelect);
    assertEquals(3, getSizeOfResultSet(rs));
    String queryID4 = rs.unwrap(SnowflakeResultSet.class).getQueryID();
    assertNotEquals(queryID4, queryID1);
    rs.close();

    statement.close();
    connection.close();
  }

  @Test
  public void testExecuteInsert() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_insert(cola number)");

    String insertSQL = "insert into test_insert values(2),(3)";
    int updateCount;
    boolean success;
    updateCount = statement.executeUpdate(insertSQL);
    assertEquals(2, updateCount);

    success = statement.execute(insertSQL);
    assertFalse(success);
    assertEquals(2, statement.getUpdateCount());
    assertEquals(2L, statement.getLargeUpdateCount());
    assertNull(statement.getResultSet());

    ResultSet rs = statement.executeQuery("select count(*) from test_insert");
    rs.next();
    assertEquals(4, rs.getInt(1));
    rs.close();

    assertTrue(statement.execute("select 1"));
    ResultSet rs0 = statement.getResultSet();
    rs0.next();
    assertEquals(rs0.getInt(1), 1);
    rs0.close();

    statement.execute("drop table if exists test_insert");
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
  public void testExecuteMerge() throws SQLException {
    Connection connection = getConnection();
    String mergeSQL =
        "merge into target using source on target.id = source.id "
            + "when matched and source.sb =22 then update set ta = 'newStr' "
            + "when not matched then insert (ta, tb) values (source.sa, source.sb)";
    Statement statement = connection.createStatement();
    statement.execute("create or replace table target(id integer, ta string, tb integer)");
    statement.execute("create or replace table source(id integer, sa string, sb integer)");
    statement.execute("insert into target values(1, 'str', 1)");
    statement.execute("insert into target values(2, 'str', 2)");
    statement.execute("insert into target values(3, 'str', 3)");
    statement.execute("insert into source values(1, 'str1', 11)");
    statement.execute("insert into source values(2, 'str2', 22)");
    statement.execute("insert into source values(3, 'str3', 33)");

    int updateCount = statement.executeUpdate(mergeSQL);

    assertEquals(1, updateCount);

    statement.execute("drop table if exists target");
    statement.execute("drop table if exists source");
    statement.close();
    connection.close();
  }

  /**
   * Test Autogenerated key.
   *
   * @throws Throwable if any error occurs
   */
  @Test
  public void testAutogenerateKey() throws Throwable {
    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      statement.execute("create or replace table t(c1 int)");
      statement.execute("insert into t values(1)", Statement.NO_GENERATED_KEYS);
      try {
        statement.execute("insert into t values(2)", Statement.RETURN_GENERATED_KEYS);
        fail("no autogenerate key is supported");
      } catch (SQLFeatureNotSupportedException ex) {
        // nop
      }
      // empty result
      ResultSet rset = statement.getGeneratedKeys();
      assertFalse(rset.next());
      rset.close();
    }
  }

  @Test
  public void testExecuteMultiInsert() throws SQLException {
    Connection connection = getConnection();
    String multiInsertionSQL =
        " insert all "
            + "into foo "
            + "into foo1 "
            + "into bar (b1, b2, b3) values (s3, s2, s1) "
            + "select s1, s2, s3 from source";

    Statement statement = connection.createStatement();
    assertFalse(
        statement.execute("create or replace table foo (f1 integer, f2 integer, f3 integer)"));
    assertFalse(
        statement.execute("create or replace table foo1 (f1 integer, f2 integer, f3 integer)"));
    assertFalse(
        statement.execute("create or replace table bar (b1 integer, b2 integer, b3 integer)"));
    assertFalse(
        statement.execute("create or replace table source(s1 integer, s2 integer, s3 integer)"));
    assertFalse(statement.execute("insert into source values(1, 2, 3)"));
    assertFalse(statement.execute("insert into source values(11, 22, 33)"));
    assertFalse(statement.execute("insert into source values(111, 222, 333)"));

    int updateCount = statement.executeUpdate(multiInsertionSQL);
    assertEquals(9, updateCount);

    statement.execute("drop table if exists foo");
    statement.execute("drop table if exists foo1");
    statement.execute("drop table if exists bar");
    statement.execute("drop table if exists source");

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
  public void testExecuteLargeBatch() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    /**
     * Generate a table with several rows and 1 column named test_large_batch Note: to truly test
     * that executeLargeBatch works with a number of rows greater than MAX_INT, replace rowcount =>
     * 15 in next code line with rowcount => 2147483648, or some other number larger than MAX_INT.
     * Test will take about 15 minutes to run.
     */
    statement.execute(
        "create or replace table test_large_batch (a number) as (select * from (select 5 from table"
            + "(generator(rowcount => 15)) v));");
    // update values in table so that all rows are updated
    statement.addBatch("update test_large_batch set a = 7 where a = 5;");
    long[] rowsUpdated = statement.executeLargeBatch();
    assertThat(rowsUpdated.length, is(1));
    long testVal = 15L;
    assertThat(rowsUpdated[0], is(testVal));
    statement.clearBatch();
    /**
     * To test SQLException for integer overflow when using executeBatch() for row updates of larger
     * than MAX_INT, uncomment the following lines of code. Test will take about 15 minutes to run.
     *
     * <p>statement.execute("create or replace table test_large_batch (a number) as (select * from
     * (select 5 from table" + "(generator(rowcount => 2147483648)) v));");
     * statement.addBatch("update test_large_batch set a = 7 where a = 5;"); try { int[] rowsUpdated
     * = statement.executeBatch(); fail(); } catch (SnowflakeSQLException e) { assertEquals((int)
     * ErrorCode.EXECUTE_BATCH_INTEGER_OVERFLOW.getMessageCode(), e.getErrorCode());
     * assertEquals(ErrorCode.EXECUTE_BATCH_INTEGER_OVERFLOW.getSqlState(), e.getSQLState()); }
     * statement.clearBatch();
     */
    statement.execute("drop table if exists test_large_batch");
    statement.close();
    connection.close();
  }

  /**
   * Mainly tested which types of statement CAN be executed by executeUpdate
   *
   * @throws SQLException if any error occurs
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testExecuteUpdateZeroCount() throws SQLException {
    try (Connection connection = getConnection()) {

      String[] testCommands = {
        "use role accountadmin",
        "use database testdb",
        "use schema testschema",
        "create or replace table testExecuteUpdate(cola number)",
        "comment on table testExecuteUpdate is 'comments'",
        "alter table testExecuteUpdate rename column cola to colb",
        "create or replace role testRole",
        "create or replace user testuser",
        "grant SELECT on table testExecuteUpdate to role testrole",
        "grant role testrole to user testuser",
        "revoke SELECT on table testExecuteUpdate from role testrole",
        "revoke role testrole from user testuser",
        "truncate table testExecuteUpdate",
        "alter session set autocommit=false",
        "alter session unset autocommit",
        "drop table if exists testExecuteUpdate",
        "set v1 = 10",
        "unset v1",
        "undrop table testExecuteUpdate"
      };
      try {
        for (String testCommand : testCommands) {
          Statement statement = connection.createStatement();
          int updateCount = statement.executeUpdate(testCommand);
          assertThat(updateCount, is(0));
          statement.close();
        }
      } finally {
        Statement statement = connection.createStatement();
        statement.execute("use role accountadmin");
        statement.execute("drop table if exists testExecuteUpdate");
        statement.execute("drop role if exists testrole");
        statement.execute("drop user if exists testuser");
      }
    }
  }

  @Test
  public void testExecuteUpdateFail() throws Exception {
    try (Connection connection = getConnection()) {

      String[] testCommands = {
        "list @~",
        "ls @~",
        "remove @~/testfile",
        "rm @~/testfile",
        "select 1",
        "show databases",
        "desc database " + AbstractDriverIT.getConnectionParameters().get("database")
      };

      for (String testCommand : testCommands) {
        try {
          Statement statement = connection.createStatement();
          statement.executeUpdate(testCommand);
          fail("TestCommand: " + testCommand + " is expected to be failed to execute");
        } catch (SQLException e) {
          assertThat(
              testCommand,
              e.getErrorCode(),
              is(ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API.getMessageCode()));
        }
      }
    }
  }

  @Test
  public void testTelemetryBatch() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    ResultSet rs;
    String sqlSelect = "select seq4() from table(generator(rowcount=>3))";
    statement.execute(sqlSelect);

    rs = statement.getResultSet();
    assertEquals(3, getSizeOfResultSet(rs));
    assertEquals(-1, statement.getUpdateCount());
    assertEquals(-1L, statement.getLargeUpdateCount());

    rs = statement.executeQuery(sqlSelect);
    assertEquals(3, getSizeOfResultSet(rs));
    rs.close();

    Telemetry telemetryClient =
        ((SnowflakeStatementV1) statement).connection.getSfSession().getTelemetryClient();

    // there should be logs ready to be sent
    assertTrue(((TelemetryClient) telemetryClient).bufferSize() > 0);

    statement.close();

    // closing the statement should flush the buffer, however, flush is async,
    // sleep some time before check buffer size
    try {
      Thread.sleep(1000);
    } catch (Throwable e) {
    }
    assertEquals(((TelemetryClient) telemetryClient).bufferSize(), 0);
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
  public void testCallStoredProcedure() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute(
        "create or replace procedure SP()\n"
            + "returns string not null\n"
            + "language javascript\n"
            + "as $$\n"
            + "  snowflake.execute({sqlText:'select seq4() from table(generator(rowcount=>5))'});\n"
            + "  return 'done';\n"
            + "$$");

    assertTrue(statement.execute("call SP()"));
    ResultSet rs = statement.getResultSet();
    assertNotNull(rs);
    assertTrue(rs.next());
    assertEquals("done", rs.getString(1));
    assertFalse(rs.next());
    assertFalse(statement.getMoreResults());
    assertEquals(-1, statement.getUpdateCount());
    assertEquals(-1L, statement.getLargeUpdateCount());

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

  @Test
  public void testUnwrapper() throws Throwable {
    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      if (statement.isWrapperFor(SnowflakeStatementV1.class)) {
        statement.execute("select 1");
        SnowflakeStatement sfstatement = statement.unwrap(SnowflakeStatement.class);
        assertNotNull(sfstatement.getQueryID());
      } else {
        fail("should be able to unwrap");
      }
      try {
        statement.unwrap(SnowflakeConnectionV1.class);
        fail("should fail to cast");
      } catch (SQLException ex) {
        // nop
      }
    }
  }

  @Test
  public void testQueryIdIsNullOnFreshStatement() throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = con.createStatement()) {
        assertNull(stmt.unwrap(SnowflakeStatement.class).getQueryID());
      }
    }
  }
}
