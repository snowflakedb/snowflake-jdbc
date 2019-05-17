/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnTravisCI;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.common.core.SqlState;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;

import static net.snowflake.client.jdbc.ErrorCode.ROW_DOES_NOT_EXIST;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Statement tests
 */
public class StatementIT extends BaseJDBCTest
{
  private void enableMultiStmt(Connection connection) throws SQLException
  {
    Statement statement = connection.createStatement();
    statement.execute("alter session set ENABLE_MULTISTATEMENT=true");
    statement.close();
  }

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testFetchDirection() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
    try
    {
      statement.setFetchDirection(ResultSet.FETCH_REVERSE);
    }
    catch (SQLFeatureNotSupportedException e)
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

    statement.setMaxRows(0);
    rs = statement.executeQuery(sqlSelect);
    assertEquals(3, getSizeOfResultSet(rs));

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
    }
    catch (SQLException e)
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
  public void testExecuteSelect() throws SQLException
  {
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
  public void testExecuteCreateAndDrop() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    boolean success = statement.execute("create or replace table test_create(colA integer)");
    assertFalse(success);
    assertEquals(-1, statement.getUpdateCount());
    assertNull(statement.getResultSet());

    int rowCount = statement.executeUpdate("create or replace table test_create_2(colA integer)");
    assertEquals(0, rowCount);
    assertEquals(-1, statement.getUpdateCount());

    success = statement.execute("drop table if exists TEST_CREATE");
    assertFalse(success);
    assertEquals(-1, statement.getUpdateCount());
    assertNull(statement.getResultSet());

    rowCount = statement.executeUpdate("drop table if exists TEST_CREATE_2");
    assertEquals(0, rowCount);
    assertEquals(-1, statement.getUpdateCount());
    assertNull(statement.getResultSet());

    statement.close();
    connection.close();
  }

  @Test
  public void testExecuteInsert() throws SQLException
  {
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
  public void testExecuteUpdateAndDelete() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    statement.execute("create or replace table test_update(cola number, colb string) " +
                      "as select 1, 'str1'");

    statement.execute("insert into test_update values(2, 'str2')");

    int updateCount;
    boolean success;
    updateCount = statement.executeUpdate("update test_update set COLB = 'newStr' where COLA = 1");
    assertEquals(1, updateCount);

    success = statement.execute("update test_update set COLB = 'newStr' where COLA = 2");
    assertFalse(success);
    assertEquals(1, statement.getUpdateCount());
    assertNull(statement.getResultSet());

    updateCount = statement.executeUpdate("delete from test_update where colA = 1");
    assertEquals(1, updateCount);

    success = statement.execute("delete from test_update where colA = 2");
    assertFalse(success);
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

  /**
   * Test Autogenerated key.
   *
   * @throws Throwable if any error occurs
   */
  @Test
  public void testAutogenerateKey() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      Statement statement = connection.createStatement();
      statement.execute("create or replace table t(c1 int)");
      statement.execute("insert into t values(1)", Statement.NO_GENERATED_KEYS);
      try
      {
        statement.execute("insert into t values(2)", Statement.RETURN_GENERATED_KEYS);
        fail("no autogenerate key is supported");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      // empty result
      ResultSet rset = statement.getGeneratedKeys();
      assertFalse(rset.next());
      rset.close();
    }
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
    assertFalse(statement.execute("create or replace table foo (f1 integer, f2 integer, f3 integer)"));
    assertFalse(statement.execute("create or replace table foo1 (f1 integer, f2 integer, f3 integer)"));
    assertFalse(statement.execute("create or replace table bar (b1 integer, b2 integer, b3 integer)"));
    assertFalse(statement.execute("create or replace table source(s1 integer, s2 integer, s3 integer)"));
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
  public void testCopyAndUpload() throws Exception
  {
    String fileName = "test_copy.csv";
    URL resource = StatementIT.class.getResource(fileName);

    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    statement.execute("create or replace table test_copy(c1 number, c2 number, c3 string)");
    assertEquals(-1, statement.getUpdateCount());

    // put files
    ResultSet rset =
        statement.executeQuery("PUT file://" + resource.getFile() + " @%test_copy");
    try
    {
      rset.getString(1);
      fail("Should raise No row found exception, because no next() is called.");
    }
    catch (SQLException ex)
    {
      assertThat("No row found error", ex.getErrorCode(),
                 equalTo(ROW_DOES_NOT_EXIST.getMessageCode()));
    }
    int cnt = 0;
    while (rset.next())
    {
      assertThat("uploaded file name",
                 rset.getString(1), equalTo(fileName));
      ++cnt;
    }
    assertEquals(-1, statement.getUpdateCount());
    assertThat("number of files", cnt, equalTo(1));
    int numRows =
        statement.executeUpdate("copy into test_copy");
    assertEquals(2, numRows);
    assertEquals(2, statement.getUpdateCount());

    statement.execute("drop table if exists test_copy");

    statement.close();
    connection.close();
  }

  @Test
  public void testExecuteBatch() throws Exception
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    connection.setAutoCommit(false);
    // mixed of ddl/dml in batch
    statement.addBatch("create or replace table test_batch(a string, b integer)");
    statement.addBatch("insert into test_batch values('str1', 1), ('str2', 2)");
    statement.addBatch("update test_batch set test_batch.b = src.b + 5 from " +
                       "(select 'str1' as a, 2 as b) src where test_batch.a = src.a");

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
    try
    {
      statement.addBatch("insert into test_batch values('str3', 3)");
      statement.addBatch("select * from test_batch");
      statement.addBatch("select * from test_batch_not_exist");
      statement.addBatch("insert into test_batch values('str4', 4)");
      statement.executeBatch();
      fail();
    }
    catch (BatchUpdateException e)
    {
      rowCounts = e.getUpdateCounts();
      assertThat(e.getErrorCode(), is(
          ERROR_CODE_DOMAIN_OBJECT_DOES_NOT_EXIST));
      assertThat(rowCounts[0], is(1));
      assertThat(rowCounts[1], is(Statement.SUCCESS_NO_INFO));
      assertThat(rowCounts[2], is(Statement.EXECUTE_FAILED));
      assertThat(rowCounts[3], is(1));

      connection.rollback();
    }

    statement.clearBatch();

    statement.addBatch("put file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @%test_batch auto_compress=false");
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
  public void testExecuteUpdateFail() throws SQLException
  {
    Connection connection = getConnection();

    String[] testCommands = {
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
      }
      catch (SQLException e)
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

    ResultSet rs;
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
  public void testCallStoredProcedure() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
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

  @Test
  public void testCreateStatementWithParameters() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      try
      {
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        fail("updateable cursor is not supported.");
      }
      catch (SQLException ex)
      {
        assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());
      }
      connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                                 ResultSet.CLOSE_CURSORS_AT_COMMIT);
      try
      {
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                                   ResultSet.HOLD_CURSORS_OVER_COMMIT);
        fail("hold cursor over commit is not supported.");
      }
      catch (SQLException ex)
      {
        assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());
      }

    }
  }

  @Test
  public void testUnwrapper() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      Statement statement = connection.createStatement();
      if (statement.isWrapperFor(SnowflakeStatementV1.class))
      {
        statement.execute("select 1");
        SnowflakeStatement sfstatement = statement.unwrap(SnowflakeStatement.class);
        assertNotNull(sfstatement.getQueryID());
      }
      else
      {
        fail("should be able to unwrap");
      }
      try
      {
        statement.unwrap(SnowflakeConnectionV1.class);
        fail("should fail to cast");
      }
      catch (SQLException ex)
      {
        // nop
      }
    }
  }
}
