/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnTravisCI;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.common.core.SqlState;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Statement integration tests
 */
public class StatementIT extends BaseJDBCTest
{
  public void enableMultiStmt(Connection connection) throws SQLException
  {
    Statement statement = connection.createStatement();
    statement.execute("alter session set ENABLE_MULTISTATEMENT=true");
    statement.close();
  }

  @Test
  public void testFetchDirection() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
    try
    {
      statement.setFetchDirection(ResultSet.FETCH_REVERSE);
    } catch (SQLFeatureNotSupportedException e)
    {
      assertTrue(true);
    }
    statement.close();
    connection.close();
  }

  @Ignore("Not working for setFetchSize")
  @Test
  public void testFetchSize() throws SQLException
  {
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
  public void testMaxRows() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String sqlSelect = "select seq4() from table(generator(rowcount=>3))";
    assertEquals(0, statement.getMaxRows());

    statement.setMaxRows(1);
    assertEquals(1, statement.getMaxRows());
    ResultSet rs = statement.executeQuery(sqlSelect);
    int resultSizeCount = getSizeOfResultSet(rs);
    assertEquals(1, resultSizeCount);
    statement.close();

    statement.setMaxRows(0);
    rs = statement.executeQuery(sqlSelect);
    assertEquals(3, getSizeOfResultSet(rs));
    statement.close();

    statement.setMaxRows(-1);
    rs = statement.executeQuery(sqlSelect);
    assertEquals(3, getSizeOfResultSet(rs));
    statement.close();

    connection.close();
  }

  @Test
  public void testQueryTimeOut() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    assertEquals(0, statement.getQueryTimeout());
    statement.setQueryTimeout(5);
    assertEquals(5, statement.getQueryTimeout());
    try
    {
      statement.executeQuery("select count(*) from table(generator(timeLimit => 100))");
    } catch (SQLException e)
    {
      assertTrue(true);
      assertEquals(SqlState.QUERY_CANCELED, e.getSQLState());
      assertEquals("SQL execution canceled", e.getMessage());
    }
    statement.close();
    connection.close();
  }

  @Test
  public void testStatementClose() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    assertEquals(connection, statement.getConnection());
    assertTrue(!statement.isClosed());
    statement.close();
    assertTrue(statement.isClosed());
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testExecuteSelect() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("alter session set JDBC_EXECUTE_RETURN_COUNT_FOR_DML = true");

    String sqlSelect = "select seq4() from table(generator(rowcount=>3))";
    boolean success = statement.execute(sqlSelect);
    assertEquals(true, success);
    ResultSet rs = statement.getResultSet();
    assertEquals(3, getSizeOfResultSet(rs));
    assertEquals(-1, statement.getUpdateCount());

    rs = statement.executeQuery(sqlSelect);
    assertEquals(3, getSizeOfResultSet(rs));
    rs.close();

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testExecuteCreateAndDrop() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("alter session set JDBC_EXECUTE_RETURN_COUNT_FOR_DML = true");

    boolean success = statement.execute("create or replace table test_create(colA integer)");
    assertEquals(false, success);
    assertEquals(0, statement.getUpdateCount());
    assertNull(statement.getResultSet());

    int updateCount = statement.executeUpdate("create or replace table test_create_2(colA integer)");
    assertEquals(0, updateCount);

    success = statement.execute("drop table if exists TEST_CREATE");
    assertEquals(false, success);
    assertEquals(0, statement.getUpdateCount());
    assertNull(statement.getResultSet());

    updateCount = statement.executeUpdate("drop table if exists TEST_CREATE_2");
    assertEquals(0, updateCount);
    assertNull(statement.getResultSet());

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testExecuteInsert() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("alter session set JDBC_EXECUTE_RETURN_COUNT_FOR_DML = true");

    statement.execute("create or replace table test_insert(cola number)");

    String insertSQL = "insert into test_insert values(2)";
    int updateCount;
    boolean success;
    updateCount = statement.executeUpdate(insertSQL);
    assertEquals(1, updateCount);

    success = statement.execute(insertSQL);
    assertEquals(false, success);
    assertEquals(1, statement.getUpdateCount());
    assertNull(statement.getResultSet());

    ResultSet rs = statement.executeQuery("select count(*) from test_insert");
    rs.next();
    assertEquals(2, rs.getInt(1));

    rs.close();

    statement.execute("drop table if exists test_insert");
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testExecuteUpdateAndDelete() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("alter session set JDBC_EXECUTE_RETURN_COUNT_FOR_DML = true");

    statement.execute("create or replace table test_update(cola number, colb string) " +
        "as select 1, 'str1'");

    statement.execute("insert into test_update values(2, 'str2')");

    int updateCount;
    boolean success;
    updateCount = statement.executeUpdate("update test_update set COLB = 'newStr' where COLA = 1");
    assertEquals(1, updateCount);

    success = statement.execute("update test_update set COLB = 'newStr' where COLA = 2");
    assertEquals(false, success);
    assertEquals(1, statement.getUpdateCount());
    assertNull(statement.getResultSet());

    updateCount = statement.executeUpdate("delete from test_update where colA = 1");
    assertEquals(1, updateCount);

    success = statement.execute("delete from test_update where colA = 2");
    assertEquals(false, success);
    assertEquals(1, statement.getUpdateCount());
    assertNull(statement.getResultSet());

    statement.execute("drop table if exists test_update");
    statement.close();

    connection.close();
  }

  @Test
  public void testExecuteMerge() throws SQLException
  {
    Connection connection = getConnection();
    String mergeSQL = "merge into target using source on target.id = source.id "
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

  @Test
  public void testExecuteMultiInsert() throws SQLException
  {
    Connection connection = getConnection();
    String multiInsertionSQL = " insert all "
        + "into foo "
        + "into foo1 "
        + "into bar (b1, b2, b3) values (s3, s2, s1) "
        + "select s1, s2, s3 from source";

    Statement statement = connection.createStatement();
    statement.execute("create or replace table foo (f1 integer, f2 integer, f3 integer)");
    statement.execute("create or replace table foo1 (f1 integer, f2 integer, f3 integer)");
    statement.execute("create or replace table bar (b1 integer, b2 integer, b3 integer)");
    statement.execute("create or replace table source(s1 integer, s2 integer, s3 integer)");
    statement.execute("insert into source values(1, 2, 3)");
    statement.execute("insert into source values(11, 22, 33)");
    statement.execute("insert into source values(111, 222, 333)");

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
  public void testCopyAndUpload() throws Exception
  {
    URL resource = StatementIT.class.getResource("test_copy.csv");

    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    statement.execute("create or replace table test_copy(c1 number, c2 number, c3 string)");

    // put files
    boolean status =
        statement.execute("PUT file://" + resource.getFile() + " @%test_copy");

    assertTrue("put command fails", status);

    int numRows =
        statement.executeUpdate("copy into test_copy");
    assertEquals(2, numRows);

    statement.execute("drop table if exists test_copy");

    statement.close();
    connection.close();
  }

  @Test
  public void testExecuteBatch() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    connection.setAutoCommit(false);
    // mixed of ddl/dml in batch
    statement.addBatch("create or replace table test_batch(a string, b integer)");
    statement.addBatch("insert into test_batch values('str1', 1), ('str2', 2)");
    statement.addBatch("update test_batch set test_batch.b = src.b + 5 from " +
        "(select 'str1' as a, 2 as b) src where test_batch.a = src.a");

    int updateCounts[] = statement.executeBatch();
    connection.commit();

    assertThat(updateCounts.length, is(3));
    assertThat(updateCounts[0], is(0));
    assertThat(updateCounts[1], is(2));
    assertThat(updateCounts[2], is(1));

    ResultSet resultSet = statement.executeQuery("select * from test_batch order by b asc");
    resultSet.next();
    assertThat(resultSet.getInt("B"), is(2));
    resultSet.next();
    assertThat(resultSet.getInt("B"), is(7));

    statement.clearBatch();

    // one of the batch is query instead of ddl/dml
    // it should continuing processing
    try
    {
      statement.addBatch("insert into test_batch values('str3', 3)");
      statement.addBatch("select * from test_batch");
      statement.addBatch("insert into test_batch values('str4', 4)");

      statement.executeBatch();

      fail();
    } catch (BatchUpdateException e)
    {
      updateCounts = e.getUpdateCounts();
      assertThat(e.getErrorCode(), is(
          ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API
              .getMessageCode()));
      assertThat(updateCounts[0], is(1));
      assertThat(updateCounts[1], is(Statement.EXECUTE_FAILED));
      assertThat(updateCounts[0], is(1));

      connection.rollback();
    }

    statement.clearBatch();

    statement.execute("drop table if exists test_batch");
    statement.close();

    connection.close();
  }

  /**
   * Mainly tested which types of statement CAN be executed by executeUpdate
   *
   * @throws SQLException if any error occurs
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testExecuteUpdateZeroCount() throws SQLException
  {
    Connection connection = getConnection();

    String testCommands[] = {
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
    try
    {
      for (String testCommand : testCommands)
      {
        Statement statement = connection.createStatement();
        int updateCount = statement.executeUpdate(testCommand);
        assertThat(updateCount, is(0));
        statement.close();
      }
    }
    finally
    {
      Statement statement = connection.createStatement();
      statement.execute("use role accountadmin");
      statement.execute("drop table if exists testExecuteUpdate");
      statement.execute("drop role if exists testrole");
      statement.execute("drop user if exists testuser");
      connection.close();
    }
  }

  @Test
  public void testExecuteUpdateFail() throws SQLException, IOException
  {
    Connection connection = getConnection();

    String testCommands[] = {
        "put file:///tmp/testfile @~",
        "list @~",
        "ls @~",
        "get @~/testfile file:///tmp",
        "remove @~/testfile",
        "rm @~/testfile",
        "select 1",
        "show databases",
        "desc database " + AbstractDriverIT.getConnectionParameters().get("database")
    };

    for (String testCommand : testCommands)
    {
      try
      {
        Statement statement = connection.createStatement();
        statement.executeUpdate(testCommand);
        fail("TestCommand: " + testCommand + " is expected to be failed to execute");
      } catch (SQLException e)
      {
        assertThat(e.getErrorCode(), is(ErrorCode.
            UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API.getMessageCode()));
      }
    }
    connection.close();
  }

  @Test
  public void testTelemetryBatch() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    ResultSet rs = null;
    String sqlSelect = "select seq4() from table(generator(rowcount=>3))";
    statement.execute(sqlSelect);

    rs = statement.getResultSet();
    assertEquals(3, getSizeOfResultSet(rs));
    assertEquals(-1, statement.getUpdateCount());

    rs = statement.executeQuery(sqlSelect);
    assertEquals(3, getSizeOfResultSet(rs));
    rs.close();

    Telemetry telemetryClient = ((SnowflakeStatementV1) statement).connection.getSfSession().getTelemetryClient();
    assertTrue(telemetryClient.bufferSize() > 0); // there should be logs ready to be sent

    statement.close();

    assertEquals(telemetryClient.bufferSize(), 0); // closing the statement should flush the buffer

    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtTransaction() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();

    statement.execute("create or replace table test_multi_txn(c1 number, c2 string)" +
        " as select 10, 'z'");

    String multiStmtQuery = "begin;\n" +
        "delete from test_multi_txn;\n" +
        "insert into test_multi_txn values (1, 'a'), (2, 'b');\n" +
        "commit";

    boolean hasResultSet = statement.execute(multiStmtQuery);
    // first statement
    assertFalse(hasResultSet);
    assertNull(statement.getResultSet());
    assertEquals(0, statement.getUpdateCount());

    // second statement
    assertFalse(statement.getMoreResults());
    assertNull(statement.getResultSet());
    assertEquals(1, statement.getUpdateCount());

    // third statement
    assertFalse(statement.getMoreResults());
    assertNull(statement.getResultSet());
    assertEquals(2, statement.getUpdateCount());

    // fourth statement
    assertFalse(statement.getMoreResults());
    assertNull(statement.getResultSet());
    assertEquals(0, statement.getUpdateCount());

    assertFalse(statement.getMoreResults());
    assertEquals(-1, statement.getUpdateCount());

    statement.close();

    statement.execute("drop table if exists test_multi_txn");
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtTransactionRollback() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();

    statement.execute("create or replace table test_multi_txn_rb(c1 number, c2 string)" +
        " as select 10, 'z'");

    String multiStmtQuery = "begin;\n" +
        "delete from test_multi_txn_rb;\n" +
        "rollback;\n" +
        "select count(*) from test_multi_txn_rb";

    boolean hasResultSet = statement.execute(multiStmtQuery);
    // first statement
    assertFalse(hasResultSet);
    assertNull(statement.getResultSet());
    assertEquals(0, statement.getUpdateCount());

    // second statement
    assertFalse(statement.getMoreResults());
    assertNull(statement.getResultSet());
    assertEquals(1, statement.getUpdateCount());

    // third statement
    assertFalse(statement.getMoreResults());
    assertNull(statement.getResultSet());
    assertEquals(0, statement.getUpdateCount());

    // fourth statement
    assertTrue(statement.getMoreResults());
    assertEquals(-1, statement.getUpdateCount());
    ResultSet rs = statement.getResultSet();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertFalse(rs.next());

    assertFalse(statement.getMoreResults());
    assertEquals(-1, statement.getUpdateCount());

    statement.execute("drop table if exists test_multi_txn_rb");
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtExecute() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();
    String multiStmtQuery = "create or replace temporary table test_multi (cola int);\n" +
        "insert into test_multi VALUES (1), (2);\n" +
        "select cola from test_multi order by cola asc";

    boolean hasResultSet = statement.execute(multiStmtQuery);
    // first statement
    assertFalse(hasResultSet);
    assertNull(statement.getResultSet());
    assertEquals(0, statement.getUpdateCount());

    // second statement
    assertFalse(statement.getMoreResults());
    assertNull(statement.getResultSet());
    assertEquals(2, statement.getUpdateCount());

    // third statement
    assertTrue(statement.getMoreResults());
    assertEquals(-1, statement.getUpdateCount());
    ResultSet rs = statement.getResultSet();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertFalse(rs.next());

    assertFalse(statement.getMoreResults());
    assertEquals(-1, statement.getUpdateCount());

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtExecuteUpdate() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();
    String multiStmtQuery = "create or replace temporary table test_multi (cola int);\n" +
        "insert into test_multi VALUES (1), (2);\n" +
        "select cola from test_multi order by cola asc";

    int updateCount = statement.executeUpdate(multiStmtQuery);
    // first statement
    assertEquals(0, updateCount);
    assertNull(statement.getResultSet());
    assertEquals(0, statement.getUpdateCount());

    // second statement
    assertFalse(statement.getMoreResults());
    assertNull(statement.getResultSet());
    assertEquals(2, statement.getUpdateCount());

    // third statement
    assertTrue(statement.getMoreResults());
    assertEquals(-1, statement.getUpdateCount());
    ResultSet rs = statement.getResultSet();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertFalse(rs.next());

    assertFalse(statement.getMoreResults());
    assertEquals(-1, statement.getUpdateCount());

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtExecuteQuery() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();
    String multiStmtQuery = "select 1;\n" +
        "create or replace temporary table test_multi (cola int);\n" +
        "insert into test_multi VALUES (1), (2);\n" +
        "select cola from test_multi order by cola asc";

    ResultSet rs = statement.executeQuery(multiStmtQuery);
    // first statement
    assertNotNull(rs);
    assertNotNull(statement.getResultSet());
    assertEquals(-1, statement.getUpdateCount());
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertFalse(rs.next());

    // second statement
    assertFalse(statement.getMoreResults());
    assertNull(statement.getResultSet());
    assertEquals(0, statement.getUpdateCount());

    // third statement
    assertFalse(statement.getMoreResults());
    assertNull(statement.getResultSet());
    assertEquals(2, statement.getUpdateCount());

    // fourth statement
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

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtExecuteUpdateFail() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();
    String multiStmtQuery = "select 1;\n" +
        "create or replace temporary table test_multi (cola int);\n" +
        "insert into test_multi VALUES (1), (2);\n" +
        "select cola from test_multi order by cola asc";

    try {
      statement.executeUpdate(multiStmtQuery);
      fail("executeUpdate should have failed because the first statement yields a result set");
    } catch (SQLException ex) {
      assertThat(ex.getErrorCode(),
          is(ErrorCode.UPDATE_FIRST_RESULT_NOT_UPDATE_COUNT.getMessageCode()));
    }

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtExecuteQueryFail() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();
    String multiStmtQuery = "create or replace temporary table test_multi (cola int);\n" +
        "insert into test_multi VALUES (1), (2);\n" +
        "select cola from test_multi order by cola asc";

    try {
      statement.executeQuery(multiStmtQuery);
      fail("executeQuery should have failed because the first statement yields an update count");
    } catch (SQLException ex) {
      assertThat(ex.getErrorCode(),
          is(ErrorCode.QUERY_FIRST_RESULT_NOT_RESULT_SET.getMessageCode()));
    }

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtSetUnset() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();

    // setting session variable should propagate outside of query
    statement.execute("set testvar = 1; select 1");
    ResultSet rs = statement.executeQuery("select $testvar");
    rs.next();
    assertEquals(1, rs.getInt(1));

    // selecting unset variable should cause error
    try
    {
      statement.execute("unset testvar; select $testvar");
      fail("Expected a failure");
    }
    catch (SQLException ex)
    {
      assertEquals(SqlState.PLSQL_ERROR, ex.getSQLState());
    }

    // unsetting session variable should propagate outside of query
    try
    {
      statement.execute("select $testvar");
      fail("Expected a failure");
    }
    catch (SQLException ex)
    {
      assertEquals(SqlState.NO_DATA, ex.getSQLState());
    }

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtParseError() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();

    statement.execute("set testvar = 1");
    try
    {
      // fails in the antlr parser
      statement.execute("garbage text; set testvar = 2");
      fail("Expected a compiler error to be thrown");
    }
    catch (SQLException ex) {
      assertEquals(SqlState.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, ex.getSQLState());
    }

    ResultSet rs = statement.executeQuery("select $testvar");
    rs.next();
    assertEquals(1, rs.getInt(1));

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtExecError() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();

    try
    {
      // fails during execution (javascript invokes statement where it gets typechecked)
      statement.execute("set testvar = 1; select nonexistent_column from nonexistent_table; set testvar = 2");
      fail("Expected an execution error to be thrown");
    }
    catch (SQLException ex) {
      assertEquals(SqlState.PLSQL_ERROR, ex.getSQLState());
    }

    ResultSet rs = statement.executeQuery("select $testvar");
    rs.next();
    assertEquals( 1, rs.getInt(1));

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtTempTable() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();

    String entry = "success";
    statement.execute("create or replace temporary table test_multi (cola string); insert into test_multi values ('" + entry + "')");
    // temporary table should persist outside of the above statement
    ResultSet rs = statement.executeQuery("select * from test_multi");
    rs.next();
    assertEquals(entry, rs.getString(1));

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtUseStmt() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();

    statement.execute("use schema public; select 1");
    // current schema change should persist outside of the above statement

    SFSession session = ((SnowflakeConnectionV1) statement.getConnection()).getSfSession();
    assertEquals("PUBLIC", session.getSchema());
    ResultSet rs = statement.executeQuery("select current_schema()");
    rs.next();
    assertEquals("PUBLIC", rs.getString(1));

    statement.execute("use schema testschema; select 1");
    // current schema change should persist outside of the above statement

    session = ((SnowflakeConnectionV1) statement.getConnection()).getSfSession();
    assertEquals("TESTSCHEMA", session.getSchema());
    rs = statement.executeQuery("select current_schema()");
    rs.next();
    assertEquals("TESTSCHEMA", rs.getString(1));

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtAlterSessionParams() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();

    SFSession session = ((SnowflakeConnectionV1) statement.getConnection()).getSfSession();

    // we need an arbitrary parameter which is updated by the client after each query for this test
    String param = "AUTOCOMMIT";
    statement.execute("alter session set " + param + "=false; select 1");
    assertFalse(session.getAutoCommit());
    statement.execute("alter session set " + param + "=true; select 1");
    assertTrue(session.getAutoCommit());

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtMultiLine() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();

    // these statements should not fail
    statement.execute("select 1;\nselect 2");
    statement.execute("select \n 1; select 2");
    statement.execute("select \r\n 1; select 2");

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtQuotes() throws SQLException
  {
    // test various quotation usage and ensure they succeed
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();

    statement.execute("create or replace temporary table \"test_multi\" (cola string); select * from \"test_multi\"");
    statement.execute("create or replace temporary table `test_multi` (cola string); select * from `test_multi`");
    statement.execute("select 'str'; select 'str2'");
    statement.execute("select '\\` backticks'; select '\\\\` more `backticks`'");

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtCommitRollback() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();

    statement.execute("create or replace table test_multi (cola string)");
    statement.execute("begin");
    statement.execute("insert into test_multi values ('abc')");
    // "commit" inside multistatement commits previous DML calls
    statement.execute("insert into test_multi values ('def'); commit");
    statement.execute("rollback");
    ResultSet rs = statement.executeQuery("select count(*) from test_multi");
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));

    statement.execute("create or replace table test_multi (cola string)");
    statement.execute("begin");
    statement.execute("insert into test_multi values ('abc')");
    // "rollback" inside multistatement rolls back previous DML calls
    statement.execute("insert into test_multi values ('def'); rollback");
    statement.execute("commit");
    rs = statement.executeQuery("select count(*) from test_multi");
    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));

    statement.execute("create or replace table test_multi (cola string)");
    // open transaction inside multistatement continues after
    statement.execute("begin; insert into test_multi values ('abc')");
    statement.execute("insert into test_multi values ('def')");
    statement.execute("commit");
    rs = statement.executeQuery("select count(*) from test_multi");
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));

    statement.execute("create or replace table test_multi (cola string)");
    // open transaction inside multistatement continues after
    statement.execute("begin; insert into test_multi values ('abc')");
    statement.execute("insert into test_multi values ('def')");
    statement.execute("rollback");
    rs = statement.executeQuery("select count(*) from test_multi");
    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtCommitRollbackNoAutocommit() throws SQLException
  {
    Connection connection = getConnection();
    enableMultiStmt(connection);
    connection.setAutoCommit(false);
    Statement statement = connection.createStatement();

    statement.execute("create or replace table test_multi (cola string)");
    statement.execute("insert into test_multi values ('abc')");
    // "commit" inside multistatement commits previous DML calls
    statement.execute("insert into test_multi values ('def'); commit");
    statement.execute("rollback");
    ResultSet rs = statement.executeQuery("select count(*) from test_multi");
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));

    statement.execute("create or replace table test_multi (cola string)");
    statement.execute("insert into test_multi values ('abc')");
    // "rollback" inside multistatement rolls back previous DML calls
    statement.execute("insert into test_multi values ('def'); rollback");
    statement.execute("commit");
    rs = statement.executeQuery("select count(*) from test_multi");
    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));

    statement.execute("create or replace table test_multi (cola string)");
    // open transaction inside multistatement continues after
    statement.execute("insert into test_multi values ('abc'); insert into test_multi values ('def')");
    statement.execute("commit");
    rs = statement.executeQuery("select count(*) from test_multi");
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));

    statement.execute("create or replace table test_multi (cola string)");
    // open transaction inside multistatement continues after
    statement.execute("insert into test_multi values ('abc'); insert into test_multi values ('def')");
    statement.execute("rollback");
    rs = statement.executeQuery("select count(*) from test_multi");
    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));

    statement.close();
    connection.close();
  }


  @Test
  public void testMultiStmtNotEnabled() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String multiStmtQuery = "create or replace temporary table test_multi (cola int);\n" +
        "insert into test_multi VALUES (1), (2);\n" +
        "select cola from test_multi order by cola asc";

    try
    {
      statement.execute(multiStmtQuery);
      fail("Using a multi-statement query without the parameter set should fail");
    }
    catch (SnowflakeSQLException ex)
    {
      assertEquals(SqlState.FEATURE_NOT_SUPPORTED, ex.getSQLState());
    }

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testMultiStmtLarge() throws SQLException
  {
    // this test verifies that multiple-statement support does not break
    // with many statements
    // it also ensures that results are returned in the correct order
    Connection connection = getConnection();
    enableMultiStmt(connection);
    Statement statement = connection.createStatement();
    StringBuilder multiStmtBuilder = new StringBuilder();
    String query = "SELECT %d;";
    for (int i = 0; i < 100; i++)
    {
      multiStmtBuilder.append(String.format(query, i));
    }

    assertTrue(statement.execute(multiStmtBuilder.toString()));
    for (int i = 0; i < 100; i++)
    {
      ResultSet rs = statement.getResultSet();
      assertNotNull(rs);
      assertEquals(-1, statement.getUpdateCount());
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
      assertFalse(rs.next());

      if (i != 99)
      {
        assertTrue(statement.getMoreResults());
      }
      else
      {
        assertFalse(statement.getMoreResults());
      }

    }

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testCallStoredProcedure() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("alter session set ENABLE_JAVASCRIPT_STORED_PROCEDURE_CREATION=true");

    statement.execute("create or replace procedure SP()\n"
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

    statement.close();
    connection.close();
  }
}
