package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * MultiStatement integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the oldest supported driver to examine
 * if the tests still is not applicable. If it is applicable, move tests to MultiStatementIT so that
 * both the latest and oldest supported driver run the tests.
 */
@Tag(TestTags.STATEMENT)
public class MultiStatementLatestIT extends BaseJDBCWithSharedConnectionIT {
  protected static String queryResultFormat = "json";

  @BeforeEach
  public void setQueryResultFormat() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
  }

  @Test
  public void testMultiStmtExecute() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
      String multiStmtQuery =
          "create or replace temporary table test_multi (cola int);\n"
              + "insert into test_multi VALUES (1), (2);\n"
              + "select cola from test_multi order by cola asc";

      boolean hasResultSet = statement.execute(multiStmtQuery);
      // first statement
      assertFalse(hasResultSet);
      assertNull(statement.getResultSet());
      assertEquals(0, statement.getUpdateCount());

      // second statement
      assertTrue(statement.getMoreResults());
      assertNull(statement.getResultSet());
      assertEquals(2, statement.getUpdateCount());

      // third statement
      assertTrue(statement.getMoreResults());
      assertEquals(-1, statement.getUpdateCount());
      try (ResultSet rs = statement.getResultSet()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }
  }

  @Test
  public void testMultiStmtTransaction() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create or replace table test_multi_txn(c1 number, c2 string)" + " as select 10, 'z'");

        statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 4);
        String multiStmtQuery =
            "begin;\n"
                + "delete from test_multi_txn;\n"
                + "insert into test_multi_txn values (1, 'a'), (2, 'b');\n"
                + "commit";

        boolean hasResultSet = statement.execute(multiStmtQuery);
        // first statement
        assertFalse(hasResultSet);
        assertNull(statement.getResultSet());
        assertEquals(0, statement.getUpdateCount());

        // second statement
        assertTrue(statement.getMoreResults());
        assertNull(statement.getResultSet());
        assertEquals(1, statement.getUpdateCount());

        // third statement
        assertTrue(statement.getMoreResults());
        assertNull(statement.getResultSet());
        assertEquals(2, statement.getUpdateCount());

        // fourth statement
        assertFalse(statement.getMoreResults());
        assertNull(statement.getResultSet());
        assertEquals(0, statement.getUpdateCount());

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

      } finally {
        statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
        statement.execute("drop table if exists test_multi_txn");
      }
    }
  }

  @Test
  public void testMultiStmtExecuteUpdate() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String multiStmtQuery =
          "create or replace temporary table test_multi (cola int);\n"
              + "insert into test_multi VALUES (1), (2);\n"
              + "select cola from test_multi order by cola asc";

      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
      int rowCount = statement.executeUpdate(multiStmtQuery);
      // first statement
      assertEquals(0, rowCount);
      assertNull(statement.getResultSet());
      assertEquals(0, statement.getUpdateCount());

      // second statement
      assertTrue(statement.getMoreResults());
      assertNull(statement.getResultSet());
      assertEquals(2, statement.getUpdateCount());

      // third statement
      assertTrue(statement.getMoreResults());
      assertEquals(-1, statement.getUpdateCount());
      try (ResultSet rs = statement.getResultSet()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }
  }

  @Test
  public void testMultiStmtTransactionRollback() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create or replace table test_multi_txn_rb(c1 number, c2 string)"
                + " as select 10, 'z'");

        statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 4);
        String multiStmtQuery =
            "begin;\n"
                + "delete from test_multi_txn_rb;\n"
                + "rollback;\n"
                + "select count(*) from test_multi_txn_rb";

        boolean hasResultSet = statement.execute(multiStmtQuery);
        // first statement
        assertFalse(hasResultSet);
        assertNull(statement.getResultSet());
        assertEquals(0, statement.getUpdateCount());

        // second statement
        assertTrue(statement.getMoreResults());
        assertNull(statement.getResultSet());
        assertEquals(1, statement.getUpdateCount());

        // third statement
        assertTrue(statement.getMoreResults());
        assertNull(statement.getResultSet());
        assertEquals(0, statement.getUpdateCount());

        // fourth statement
        assertTrue(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
        try (ResultSet rs = statement.getResultSet()) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          assertFalse(rs.next());

          assertFalse(statement.getMoreResults());
          assertEquals(-1, statement.getUpdateCount());
        }
      } finally {
        statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 1);
        statement.execute("drop table if exists test_multi_txn_rb");
      }
    }
  }

  @Test
  public void testMultiStmtExecuteQuery() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String multiStmtQuery =
          "select 1;\n"
              + "create or replace temporary table test_multi (cola int);\n"
              + "insert into test_multi VALUES (1), (2);\n"
              + "select cola from test_multi order by cola asc";

      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 4);
      try (ResultSet rs = statement.executeQuery(multiStmtQuery)) {
        // first statement
        assertNotNull(rs);
        assertNotNull(statement.getResultSet());
        assertEquals(-1, statement.getUpdateCount());
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        // second statement
        assertTrue(statement.getMoreResults());
        assertNull(statement.getResultSet());
        assertEquals(0, statement.getUpdateCount());

        // third statement
        assertTrue(statement.getMoreResults());
        assertNull(statement.getResultSet());
        assertEquals(2, statement.getUpdateCount());

        // fourth statement
        assertTrue(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
      try (ResultSet rs = statement.getResultSet()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }
  }

  @Test
  public void testMultiStmtUpdateCount() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 2);
      boolean isResultSet =
          statement.execute(
              "CREATE OR REPLACE TEMPORARY TABLE TABLIST AS "
                  + "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                  + "WHERE TABLE_NAME LIKE 'K%' "
                  + "ORDER BY TABLE_SCHEMA, TABLE_NAME; "
                  + "SELECT * FROM TABLIST "
                  + "JOIN INFORMATION_SCHEMA.COLUMNS "
                  + "ON COLUMNS.TABLE_SCHEMA = TABLIST.TABLE_SCHEMA "
                  + "AND COLUMNS.TABLE_NAME = TABLIST.TABLE_NAME;");
      assertEquals(isResultSet, false);
      int statementUpdateCount = statement.getUpdateCount();
      assertEquals(statementUpdateCount, 0);
      isResultSet = statement.getMoreResults();
      assertEquals(isResultSet, true);
      statementUpdateCount = statement.getUpdateCount();
      assertEquals(statementUpdateCount, -1);
    }
  }

  /** Test use of anonymous blocks (SNOW-758262) */
  @Test
  public void testAnonymousBlocksUse() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("create or replace table tab758262(c1 number)");
      // Test anonymous block with multistatement
      int multistatementcount = 2;
      statement
          .unwrap(SnowflakeStatement.class)
          .setParameter("MULTI_STATEMENT_COUNT", multistatementcount);
      String multiStmtQuery =
          "begin\n"
              + "insert into tab758262 values (1);\n"
              + "return 'done';\n"
              + "end;\n"
              + "select * from tab758262;";

      statement.execute(multiStmtQuery);
      for (int i = 0; i < multistatementcount - 1; i++) {
        assertTrue(statement.getMoreResults());
      }
      try (ResultSet rs = statement.getResultSet()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }

      // Test anonymous block in the middle of other queries in multistatement
      multiStmtQuery =
          "insert into tab758262 values (25), (26);\n"
              + "begin\n"
              + "insert into tab758262 values (27);\n"
              + "return 'done';\n"
              + "end;\n"
              + "select * from tab758262;";
      multistatementcount = 3;
      statement
          .unwrap(SnowflakeStatement.class)
          .setParameter("MULTI_STATEMENT_COUNT", multistatementcount);
      statement.execute(multiStmtQuery);
      for (int i = 0; i < multistatementcount - 1; i++) {
        assertTrue(statement.getMoreResults());
      }
      try (ResultSet rs = statement.getResultSet()) {
        assertEquals(4, getSizeOfResultSet(rs));
      }
    }
  }
}
