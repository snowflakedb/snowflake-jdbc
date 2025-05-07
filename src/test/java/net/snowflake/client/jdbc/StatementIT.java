package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
import net.snowflake.common.core.SqlState;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Statement tests */
@Tag(TestTags.STATEMENT)
public class StatementIT extends BaseJDBCWithSharedConnectionIT {
  protected static String queryResultFormat = "json";

  public static Connection getConnection() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection();
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
    return conn;
  }

  @TempDir private File tmpFolder;

  @Test
  public void testFetchDirection() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
      assertThrows(
          SQLFeatureNotSupportedException.class,
          () -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
    }
  }

  @Disabled("Not working for setFetchSize")
  @Test
  public void testFetchSize() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      assertEquals(50, statement.getFetchSize());
      statement.setFetchSize(1);
      ResultSet rs = statement.executeQuery("select * from JDBC_STATEMENT");
      assertEquals(1, getSizeOfResultSet(rs));
    }
  }

  @Test
  public void testMaxRows() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String sqlSelect = "select seq4() from table(generator(rowcount=>3))";
      assertEquals(0, statement.getMaxRows());

      //    statement.setMaxRows(1);
      //    assertEquals(1, statement.getMaxRows());
      try (ResultSet rs = statement.executeQuery(sqlSelect)) {
        int resultSizeCount = getSizeOfResultSet(rs);
        //    assertEquals(1, resultSizeCount);
      }
      statement.setMaxRows(0);
      try (ResultSet rs = statement.executeQuery(sqlSelect)) {
        //    assertEquals(3, getSizeOfResultSet(rs));
      }
      statement.setMaxRows(-1);
      try (ResultSet rs = statement.executeQuery(sqlSelect)) {
        //    assertEquals(3, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  public void testQueryTimeOut() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      assertEquals(0, statement.getQueryTimeout());
      statement.setQueryTimeout(5);
      assertEquals(5, statement.getQueryTimeout());
      SQLException e =
          assertThrows(
              SQLException.class,
              () ->
                  statement.executeQuery(
                      "select count(*) from table(generator(timeLimit => 100))"));
      assertTrue(true);
      assertEquals(SqlState.QUERY_CANCELED, e.getSQLState());
      assertEquals("SQL execution canceled", e.getMessage());
    }
  }

  @Test
  public void testStatementClose() throws SQLException {
    try (Statement statement = connection.createStatement(); ) {
      assertEquals(connection, statement.getConnection());
      assertTrue(!statement.isClosed());
      statement.close();
      assertTrue(statement.isClosed());
    }
  }

  @Test
  public void testExecuteSelect() throws SQLException {
    try (Statement statement = connection.createStatement()) {

      String sqlSelect = "select seq4() from table(generator(rowcount=>3))";
      boolean success = statement.execute(sqlSelect);
      assertTrue(success);
      String queryID1 = statement.unwrap(SnowflakeStatement.class).getQueryID();
      assertNotNull(queryID1);

      try (ResultSet rs = statement.getResultSet()) {
        assertEquals(3, getSizeOfResultSet(rs));
        assertEquals(-1, statement.getUpdateCount());
        assertEquals(-1L, statement.getLargeUpdateCount());
        String queryID2 = rs.unwrap(SnowflakeResultSet.class).getQueryID();
        assertEquals(queryID2, queryID1);
      }
      try (ResultSet rs = statement.executeQuery(sqlSelect)) {
        assertEquals(3, getSizeOfResultSet(rs));
        String queryID4 = rs.unwrap(SnowflakeResultSet.class).getQueryID();
        assertNotEquals(queryID4, queryID1);
      }
    }
  }

  @Test
  public void testExecuteInsert() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
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

        try (ResultSet rs = statement.executeQuery("select count(*) from test_insert")) {
          assertTrue(rs.next());
          assertEquals(4, rs.getInt(1));
        }

        assertTrue(statement.execute("select 1"));
        try (ResultSet rs0 = statement.getResultSet()) {
          assertTrue(rs0.next());
          assertEquals(rs0.getInt(1), 1);
        }
      } finally {
        statement.execute("drop table if exists test_insert");
      }
    }
  }

  @Test
  public void testExecuteUpdateAndDelete() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create or replace table test_update(cola number, colb string) "
                + "as select 1, 'str1'");

        statement.execute("insert into test_update values(2, 'str2')");

        int updateCount;
        boolean success;
        updateCount =
            statement.executeUpdate("update test_update set COLB = 'newStr' where COLA = 1");
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
      } finally {
        statement.execute("drop table if exists test_update");
      }
    }
  }

  @Test
  public void testExecuteMerge() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String mergeSQL =
          "merge into target using source on target.id = source.id "
              + "when matched and source.sb =22 then update set ta = 'newStr' "
              + "when not matched then insert (ta, tb) values (source.sa, source.sb)";
      try {
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
      } finally {
        statement.execute("drop table if exists target");
        statement.execute("drop table if exists source");
      }
    }
  }

  /**
   * Test Autogenerated key.
   *
   * @throws Throwable if any error occurs
   */
  @Test
  public void testAutogenerateKey() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      statement.execute("create or replace table t(c1 int)");
      statement.execute("insert into t values(1)", Statement.NO_GENERATED_KEYS);

      assertThrows(
          SQLFeatureNotSupportedException.class,
          () -> statement.execute("insert into t values(2)", Statement.RETURN_GENERATED_KEYS));

      // empty result
      try (ResultSet rset = statement.getGeneratedKeys()) {
        assertFalse(rset.next());
      }
    }
  }

  @Test
  public void testExecuteMultiInsert() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String multiInsertionSQL =
          " insert all "
              + "into foo "
              + "into foo1 "
              + "into bar (b1, b2, b3) values (s3, s2, s1) "
              + "select s1, s2, s3 from source";

      try {
        assertFalse(
            statement.execute("create or replace table foo (f1 integer, f2 integer, f3 integer)"));
        assertFalse(
            statement.execute("create or replace table foo1 (f1 integer, f2 integer, f3 integer)"));
        assertFalse(
            statement.execute("create or replace table bar (b1 integer, b2 integer, b3 integer)"));
        assertFalse(
            statement.execute(
                "create or replace table source(s1 integer, s2 integer, s3 integer)"));
        assertFalse(statement.execute("insert into source values(1, 2, 3)"));
        assertFalse(statement.execute("insert into source values(11, 22, 33)"));
        assertFalse(statement.execute("insert into source values(111, 222, 333)"));

        int updateCount = statement.executeUpdate(multiInsertionSQL);
        assertEquals(9, updateCount);
      } finally {
        statement.execute("drop table if exists foo");
        statement.execute("drop table if exists foo1");
        statement.execute("drop table if exists bar");
        statement.execute("drop table if exists source");
      }
    }
  }

  @Test
  public void testExecuteBatch() throws Exception {
    try (Statement statement = connection.createStatement()) {
      try {
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

        try (ResultSet resultSet =
            statement.executeQuery("select * from test_batch order by b asc")) {
          assertTrue(resultSet.next());
          assertThat(resultSet.getInt("B"), is(2));
          assertTrue(resultSet.next());
          assertThat(resultSet.getInt("B"), is(7));
          statement.clearBatch();

          // one of the batch is query instead of ddl/dml
          // it should continuing processing
          statement.addBatch("insert into test_batch values('str3', 3)");
          statement.addBatch("select * from test_batch");
          statement.addBatch("select * from test_batch_not_exist");
          statement.addBatch("insert into test_batch values('str4', 4)");
          BatchUpdateException e =
              assertThrows(BatchUpdateException.class, statement::executeBatch);
          rowCounts = e.getUpdateCounts();
          assertThat(e.getErrorCode(), is(ERROR_CODE_DOMAIN_OBJECT_DOES_NOT_EXIST));
          assertThat(rowCounts[0], is(1));
          assertThat(rowCounts[1], is(Statement.SUCCESS_NO_INFO));
          assertThat(rowCounts[2], is(Statement.EXECUTE_FAILED));
          assertThat(rowCounts[3], is(1));

          connection.rollback();

          statement.clearBatch();

          statement.addBatch(
              "put file://"
                  + getFullPathFileInResource(TEST_DATA_FILE)
                  + " @%test_batch auto_compress=false");
          File tempFolder = new File(tmpFolder, "test_downloads_folder");
          tempFolder.mkdirs();
          statement.addBatch("get @%test_batch file://" + tempFolder.getCanonicalPath());

          rowCounts = statement.executeBatch();
          assertThat(rowCounts.length, is(2));
          assertThat(rowCounts[0], is(Statement.SUCCESS_NO_INFO));
          assertThat(rowCounts[0], is(Statement.SUCCESS_NO_INFO));
          statement.clearBatch();
        }
      } finally {
        statement.execute("drop table if exists test_batch");
      }
    }
  }

  @Test
  public void testExecuteLargeBatch() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      /**
       * Generate a table with several rows and 1 column named test_large_batch Note: to truly test
       * that executeLargeBatch works with a number of rows greater than MAX_INT, replace rowcount
       * => 15 in next code line with rowcount => 2147483648, or some other number larger than
       * MAX_INT. Test will take about 15 minutes to run.
       */
      try {
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
         * To test SQLException for integer overflow when using executeBatch() for row updates of
         * larger than MAX_INT, uncomment the following lines of code. Test will take about 15
         * minutes to run.
         *
         * <p>statement.execute("create or replace table test_large_batch (a number) as (select *
         * from (select 5 from table" + "(generator(rowcount => 2147483648)) v));");
         * statement.addBatch("update test_large_batch set a = 7 where a = 5;"); try { int[]
         * rowsUpdated = statement.executeBatch(); fail(); } catch (SnowflakeSQLException e) {
         * assertEquals((int) ErrorCode.EXECUTE_BATCH_INTEGER_OVERFLOW.getMessageCode(),
         * e.getErrorCode()); assertEquals(ErrorCode.EXECUTE_BATCH_INTEGER_OVERFLOW.getSqlState(),
         * e.getSQLState()); } statement.clearBatch();
         */
      } finally {
        statement.execute("drop table if exists test_large_batch");
      }
    }
  }

  /**
   * Mainly tested which types of statement CAN be executed by executeUpdate
   *
   * @throws SQLException if any error occurs
   */
  @Test
  @DontRunOnGithubActions
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
          try (Statement statement = connection.createStatement()) {
            int updateCount = statement.executeUpdate(testCommand);
            assertThat(updateCount, is(0));
          }
        }
      } finally {
        try (Statement statement = connection.createStatement()) {
          statement.execute("use role accountadmin");
          statement.execute("drop table if exists testExecuteUpdate");
          statement.execute("drop role if exists testrole");
          statement.execute("drop user if exists testuser");
        }
      }
    }
  }

  @Test
  public void testExecuteUpdateFail() throws Exception {
    try (Statement statement = connection.createStatement()) {
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
        SQLException e =
            assertThrows(
                SQLException.class,
                () -> statement.executeUpdate(testCommand),
                "TestCommand: " + testCommand + " is expected to be failed to execute");
        assertThat(
            testCommand,
            e.getErrorCode(),
            is(ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API.getMessageCode()));
      }
    }
  }

  @Test
  public void testTelemetryBatch() throws SQLException {
    Telemetry telemetryClient = null;
    try (Statement statement = connection.createStatement()) {

      String sqlSelect = "select seq4() from table(generator(rowcount=>3))";
      statement.execute(sqlSelect);

      try (ResultSet rs = statement.getResultSet()) {
        assertEquals(3, getSizeOfResultSet(rs));
        assertEquals(-1, statement.getUpdateCount());
        assertEquals(-1L, statement.getLargeUpdateCount());
      }

      try (ResultSet rs = statement.executeQuery(sqlSelect)) {
        assertEquals(3, getSizeOfResultSet(rs));
      }

      telemetryClient =
          ((SnowflakeStatementV1) statement).connection.getSfSession().getTelemetryClient();

      // there should be logs ready to be sent
      assertTrue(((TelemetryClient) telemetryClient).bufferSize() > 0);
    }

    Telemetry finalTelemetryClient = telemetryClient;
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> ((TelemetryClient) finalTelemetryClient).bufferSize(), equalTo(0));
  }

  @Test
  public void testMultiStmtNotEnabled() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String multiStmtQuery =
          "create or replace temporary table test_multi (cola int);\n"
              + "insert into test_multi VALUES (1), (2);\n"
              + "select cola from test_multi order by cola asc";

      SnowflakeSQLException ex =
          assertThrows(
              SnowflakeSQLException.class,
              () -> statement.execute(multiStmtQuery),
              "Using a multi-statement query without the parameter set should fail");
      assertEquals(SqlState.FEATURE_NOT_SUPPORTED, ex.getSQLState());
    }
  }

  @Test
  public void testCallStoredProcedure() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace procedure SP()\n"
              + "returns string not null\n"
              + "language javascript\n"
              + "as $$\n"
              + "  snowflake.execute({sqlText:'select seq4() from table(generator(rowcount=>5))'});\n"
              + "  return 'done';\n"
              + "$$");

      assertTrue(statement.execute("call SP()"));
      try (ResultSet rs = statement.getResultSet()) {
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("done", rs.getString(1));
        assertFalse(rs.next());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
        assertEquals(-1L, statement.getLargeUpdateCount());
      }
    }
  }

  @Test
  public void testCreateStatementWithParameters() throws Throwable {
    try (Connection connection = getConnection()) {
      connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      SQLException ex =
          assertThrows(
              SQLException.class,
              () ->
                  connection.createStatement(
                      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
      assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());

      connection.createStatement(
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY,
          ResultSet.CLOSE_CURSORS_AT_COMMIT);

      ex =
          assertThrows(
              SQLException.class,
              () ->
                  connection.createStatement(
                      ResultSet.TYPE_FORWARD_ONLY,
                      ResultSet.CONCUR_READ_ONLY,
                      ResultSet.HOLD_CURSORS_OVER_COMMIT));
      assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());
    }
  }

  @Test
  public void testUnwrapper() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      assertTrue(statement.isWrapperFor(SnowflakeStatementV1.class));
      statement.execute("select 1");
      SnowflakeStatement sfstatement = statement.unwrap(SnowflakeStatement.class);
      assertNotNull(sfstatement.getQueryID());

      assertThrows(SQLException.class, () -> statement.unwrap(SnowflakeConnectionV1.class));
    }
  }

  @Test
  public void testQueryIdIsNullOnFreshStatement() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      assertNull(stmt.unwrap(SnowflakeStatement.class).getQueryID());
    }
  }
}
