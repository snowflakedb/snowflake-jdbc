/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.core.SFSession;
import net.snowflake.common.core.SqlState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Multi Statement tests */
// @Category(TestCategoryStatement.class)
public class MultiStatementIT extends BaseJDBCWithSharedConnectionIT {
  protected static String queryResultFormat = "json";

  @BeforeEach
  public void setQueryResultFormat() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
  }

  @Test
  public void testMultiStmtExecuteUpdateFail() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String multiStmtQuery =
          "select 1;\n"
              + "create or replace temporary table test_multi (cola int);\n"
              + "insert into test_multi VALUES (1), (2);\n"
              + "select cola from test_multi order by cola asc";

      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 4);
      try {
        statement.executeUpdate(multiStmtQuery);
        Assertions.fail(
            "executeUpdate should have failed because the first statement yields a result set");
      } catch (SQLException ex) {
        assertThat(
            ex.getErrorCode(), is(ErrorCode.UPDATE_FIRST_RESULT_NOT_UPDATE_COUNT.getMessageCode()));
      }
    }
  }

  @Test
  public void testMultiStmtExecuteQueryFail() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String multiStmtQuery =
          "create or replace temporary table test_multi (cola int);\n"
              + "insert into test_multi VALUES (1), (2);\n"
              + "select cola from test_multi order by cola asc";

      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
      try {
        statement.executeQuery(multiStmtQuery);
        Assertions.fail(
            "executeQuery should have failed because the first statement yields an update count");
      } catch (SQLException ex) {
        assertThat(
            ex.getErrorCode(), is(ErrorCode.QUERY_FIRST_RESULT_NOT_RESULT_SET.getMessageCode()));
      }
    }
  }

  @Test
  public void testMultiStmtSetUnset() throws SQLException {
    try (Statement statement = connection.createStatement()) {

      // setting session variable should propagate outside of query
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute("set testvar = 1; select 1");

      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      try (ResultSet rs = statement.executeQuery("select $testvar")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(1, rs.getInt(1));

        // selecting unset variable should cause error
        try {
          statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
          statement.execute("unset testvar; select $testvar");
          Assertions.fail("Expected a failure");
        } catch (SQLException ex) {
          Assertions.assertEquals(SqlState.PLSQL_ERROR, ex.getSQLState());
        }

        // unsetting session variable should propagate outside of query
        try {
          statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
          statement.execute("select $testvar");
          Assertions.fail("Expected a failure");
        } catch (SQLException ex) {
          Assertions.assertEquals(SqlState.NO_DATA, ex.getSQLState());
        }
      }
    }
  }

  @Test
  public void testMultiStmtParseError() throws SQLException {
    try (Statement statement = connection.createStatement()) {

      statement.execute("set testvar = 1");
      try {
        // fails in the antlr parser
        statement.execute("garbage text; set testvar = 2");
        Assertions.fail("Expected a compiler error to be thrown");
      } catch (SQLException ex) {
        Assertions.assertEquals(SqlState.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, ex.getSQLState());
      }

      try (ResultSet rs = statement.executeQuery("select $testvar")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(1, rs.getInt(1));
      }
    }
  }

  @Test
  public void testMultiStmtExecError() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
        // fails during execution (javascript invokes statement where it gets typechecked)
        statement.execute(
            "set testvar = 1; select nonexistent_column from nonexistent_table; set testvar = 2");
        Assertions.fail("Expected an execution error to be thrown");
      } catch (SQLException ex) {
        Assertions.assertEquals(SqlState.PLSQL_ERROR, ex.getSQLState());
      }

      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      try (ResultSet rs = statement.executeQuery("select $testvar")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(1, rs.getInt(1));
      }
    }
  }

  @Test
  public void testMultiStmtTempTable() throws SQLException {
    try (Statement statement = connection.createStatement()) {

      String entry = "success";
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute(
          "create or replace temporary table test_multi (cola string); insert into test_multi values ('"
              + entry
              + "')");
      // temporary table should persist outside of the above statement
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      try (ResultSet rs = statement.executeQuery("select * from test_multi")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(entry, rs.getString(1));
      }
    }
  }

  @Test
  public void testMultiStmtUseStmt() throws SQLException {
    try (Statement statement = connection.createStatement()) {

      SFSession session =
          statement.getConnection().unwrap(SnowflakeConnectionV1.class).getSfSession();

      String originalSchema = session.getSchema();

      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute("use schema public; select 1");
      // current schema change should persist outside of the above statement

      Assertions.assertEquals("PUBLIC", session.getSchema());
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      try (ResultSet rs = statement.executeQuery("select current_schema()")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals("PUBLIC", rs.getString(1));
      }
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute(String.format("use schema %s; select 1", originalSchema));
      // current schema change should persist outside of the above statement

      session = statement.getConnection().unwrap(SnowflakeConnectionV1.class).getSfSession();
      Assertions.assertEquals(originalSchema, session.getSchema());
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      try (ResultSet rs = statement.executeQuery("select current_schema()")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(originalSchema, rs.getString(1));
      }
    }
  }

  @Test
  public void testMultiStmtAlterSessionParams() throws SQLException {
    try (Statement statement = connection.createStatement()) {

      SFSession session =
          statement.getConnection().unwrap(SnowflakeConnectionV1.class).getSfSession();

      // we need an arbitrary parameter which is updated by the client after each query for this
      // test
      String param = "AUTOCOMMIT";
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute("alter session set " + param + "=false; select 1");
      Assertions.assertFalse(session.getAutoCommit());
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute("alter session set " + param + "=true; select 1");
      Assertions.assertTrue(session.getAutoCommit());
    }
  }

  @Test
  public void testMultiStmtMultiLine() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // these statements should not fail
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute("select 1;\nselect 2");
      statement.execute("select \n 1; select 2");
      statement.execute("select \r\n 1; select 2");
    }
  }

  @Test
  public void testMultiStmtQuotes() throws SQLException {
    // test various quotation usage and ensure they succeed
    try (Statement statement = connection.createStatement()) {
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute(
          "create or replace temporary table \"test_multi\" (cola string); select * from \"test_multi\"");
      statement.execute(
          "create or replace temporary table `test_multi` (cola string); select * from `test_multi`");
      statement.execute("select 'str'; select 'str2'");
      statement.execute("select '\\` backticks'; select '\\\\` more `backticks`'");
    }
  }

  @Test
  public void testMultiStmtCommitRollback() throws SQLException {
    try (Statement statement = connection.createStatement()) {

      statement.execute("create or replace table test_multi_commit_rollback (cola string)");
      statement.execute("begin");
      statement.execute("insert into test_multi_commit_rollback values ('abc')");
      // "commit" inside multistatement commits previous DML calls
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute("insert into test_multi_commit_rollback values ('def'); commit");
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      statement.execute("rollback");
      try (ResultSet rs =
          statement.executeQuery("select count(*) from test_multi_commit_rollback")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(2, rs.getInt(1));
      }

      statement.execute("create or replace table test_multi_commit_rollback (cola string)");
      statement.execute("begin");
      statement.execute("insert into test_multi_commit_rollback values ('abc')");
      // "rollback" inside multistatement rolls back previous DML calls
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute("insert into test_multi_commit_rollback values ('def'); rollback");
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      statement.execute("commit");
      try (ResultSet rs =
          statement.executeQuery("select count(*) from test_multi_commit_rollback")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(0, rs.getInt(1));
      }
      statement.execute("create or replace table test_multi_commit_rollback (cola string)");
      // open transaction inside multistatement continues after
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute("begin; insert into test_multi_commit_rollback values ('abc')");
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      statement.execute("insert into test_multi_commit_rollback values ('def')");
      statement.execute("commit");
      try (ResultSet rs =
          statement.executeQuery("select count(*) from test_multi_commit_rollback")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(2, rs.getInt(1));
      }
      statement.execute("create or replace table test_multi_commit_rollback (cola string)");
      // open transaction inside multistatement continues after
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute("begin; insert into test_multi_commit_rollback values ('abc')");
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      statement.execute("insert into test_multi_commit_rollback values ('def')");
      statement.execute("rollback");
      try (ResultSet rs =
          statement.executeQuery("select count(*) from test_multi_commit_rollback")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  public void testMultiStmtCommitRollbackNoAutocommit() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      statement.execute("create or replace table test_multi_commit_rollback (cola string)");
      statement.execute("insert into test_multi_commit_rollback values ('abc')");
      // "commit" inside multistatement commits previous DML calls
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute("insert into test_multi_commit_rollback values ('def'); commit");
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      statement.execute("rollback");
      try (ResultSet rs =
          statement.executeQuery("select count(*) from test_multi_commit_rollback")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(2, rs.getInt(1));
      }

      statement.execute("create or replace table test_multi_commit_rollback (cola string)");
      statement.execute("insert into test_multi_commit_rollback values ('abc')");
      // "rollback" inside multistatement rolls back previous DML calls
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute("insert into test_multi_commit_rollback values ('def'); rollback");
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      statement.execute("commit");
      try (ResultSet rs =
          statement.executeQuery("select count(*) from test_multi_commit_rollback")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(0, rs.getInt(1));
      }

      statement.execute("create or replace table test_multi_commit_rollback (cola string)");
      // open transaction inside multistatement continues after
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute(
          "insert into test_multi_commit_rollback values ('abc'); insert into test_multi_commit_rollback values ('def')");
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      statement.execute("commit");
      try (ResultSet rs =
          statement.executeQuery("select count(*) from test_multi_commit_rollback")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(2, rs.getInt(1));
      }
      statement.execute("create or replace table test_multi_commit_rollback (cola string)");
      // open transaction inside multistatement continues after
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      statement.execute(
          "insert into test_multi_commit_rollback values ('abc'); insert into test_multi_commit_rollback values ('def')");
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
      statement.execute("rollback");
      try (ResultSet rs =
          statement.executeQuery("select count(*) from test_multi_commit_rollback")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  public void testMultiStmtLarge() throws SQLException {
    // this test verifies that multiple-statement support does not break
    // with many statements
    // it also ensures that results are returned in the correct order
    try (Statement statement = connection.createStatement()) {
      StringBuilder multiStmtBuilder = new StringBuilder();
      String query = "SELECT %d;";
      for (int i = 0; i < 100; i++) {
        multiStmtBuilder.append(String.format(query, i));
      }
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 100);

      Assertions.assertTrue(statement.execute(multiStmtBuilder.toString()));
      for (int i = 0; i < 100; i++) {
        try (ResultSet rs = statement.getResultSet()) {
          Assertions.assertNotNull(rs);
          Assertions.assertEquals(-1, statement.getUpdateCount());
          Assertions.assertTrue(rs.next());
          Assertions.assertEquals(i, rs.getInt(1));
          Assertions.assertFalse(rs.next());

          if (i != 99) {
            Assertions.assertTrue(statement.getMoreResults());
          } else {
            Assertions.assertFalse(statement.getMoreResults());
          }
        }
      }
    }
  }

  @Test
  public void testMultiStmtCountNotMatch() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("select 1; select 2; select 3");
        Assertions.fail();
      } catch (SQLException e) {
        assertThat(e.getErrorCode(), is(8));
      }

      try {
        statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
        statement.execute("select 1");
        Assertions.fail();
      } catch (SQLException e) {
        assertThat(e.getErrorCode(), is(8));
      }

      // 0 means any number of statement can be executed
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 0);
      statement.execute("select 1; select 2; select 3");
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testInvalidParameterCount() throws SQLException {
    String userName = null;
    String accountName = null;
    try (Statement statement = connection.createStatement()) {

      try (ResultSet rs = statement.executeQuery("select current_account_locator()")) {
        Assertions.assertTrue(rs.next());
        accountName = rs.getString(1);
      }

      try (ResultSet rs = statement.executeQuery("select current_user()")) {
        Assertions.assertTrue(rs.next());
        userName = rs.getString(1);
      }

      String[] testSuites = new String[5];
      testSuites[0] =
          String.format("alter account %s set " + "multi_statement_count = 20", accountName);
      testSuites[1] =
          String.format("alter account %s set " + "multi_statement_count = -1", accountName);
      testSuites[2] = String.format("alter user %s set " + "multi_statement_count = 20", userName);
      testSuites[3] = String.format("alter user %s set " + "multi_statement_count = -1", userName);
      testSuites[4] = "alter session set " + "multi_statement_count = -1";

      int[] expectedErrorCodes = new int[5];
      expectedErrorCodes[0] = 1008;
      expectedErrorCodes[1] = 1008;
      expectedErrorCodes[2] = 1006;
      expectedErrorCodes[3] = 1006;
      expectedErrorCodes[4] = 1008;

      statement.execute("use role accountadmin");

      for (int i = 0; i < testSuites.length; i++) {
        try {
          statement.execute(testSuites[i]);
          Assertions.fail();
        } catch (SQLException e) {
          assertThat(e.getErrorCode(), is(expectedErrorCodes[i]));
        }
      }
    }
  }
}
