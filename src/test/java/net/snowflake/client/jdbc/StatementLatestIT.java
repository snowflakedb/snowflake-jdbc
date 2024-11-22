/*
 * Copyright (c) 2022 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.ErrorCode.ROW_DOES_NOT_EXIST;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.snowflake.client.TestUtil;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.bind.BindUploader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Statement integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the oldest supported driver to examine
 * if the tests still is not applicable. If it is applicable, move tests to StatementIT so that both
 * the latest and oldest supported driver run the tests.
 */
@Tag(TestTags.STATEMENT)
public class StatementLatestIT extends BaseJDBCWithSharedConnectionIT {
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
  public void testExecuteCreateAndDrop() throws SQLException {
    try (Statement statement = connection.createStatement()) {

      boolean success = statement.execute("create or replace table test_create(colA integer)");
      assertFalse(success);
      assertEquals(0, statement.getUpdateCount());
      assertEquals(0, statement.getLargeUpdateCount());
      assertNull(statement.getResultSet());

      int rowCount = statement.executeUpdate("create or replace table test_create_2(colA integer)");
      assertEquals(0, rowCount);
      assertEquals(0, statement.getUpdateCount());

      success = statement.execute("drop table if exists TEST_CREATE");
      assertFalse(success);
      assertEquals(0, statement.getUpdateCount());
      assertEquals(0, statement.getLargeUpdateCount());
      assertNull(statement.getResultSet());

      rowCount = statement.executeUpdate("drop table if exists TEST_CREATE_2");
      assertEquals(0, rowCount);
      assertEquals(0, statement.getUpdateCount());
      assertEquals(0, statement.getLargeUpdateCount());
      assertNull(statement.getResultSet());
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testCopyAndUpload() throws Exception {
    File tempFolder = new File(tmpFolder, "test_downloads_folder");
    tempFolder.mkdirs();
    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      String fileName = "test_copy.csv";
      URL resource = StatementIT.class.getResource(fileName);

      try (Connection connection = getConnection(accounts.get(i));
          Statement statement = connection.createStatement()) {
        try {
          statement.execute("create or replace table test_copy(c1 number, c2 number, c3 string)");
          assertEquals(0, statement.getUpdateCount());
          assertEquals(0, statement.getLargeUpdateCount());

          String path = resource.getFile();

          // put files
          try (ResultSet rset = statement.executeQuery("PUT file://" + path + " @%test_copy")) {
            try {
              rset.getString(1);
              fail("Should raise No row found exception, because no next() is called.");
            } catch (SQLException ex) {
              assertThat(
                  "No row found error",
                  ex.getErrorCode(),
                  equalTo(ROW_DOES_NOT_EXIST.getMessageCode()));
            }
            int cnt = 0;
            while (rset.next()) {
              assertThat("uploaded file name", rset.getString(1), equalTo(fileName));
              ++cnt;
            }
            assertEquals(0, statement.getUpdateCount());
            assertEquals(0, statement.getLargeUpdateCount());
            assertThat("number of files", cnt, equalTo(1));
            int numRows = statement.executeUpdate("copy into test_copy");
            assertEquals(2, numRows);
            assertEquals(2, statement.getUpdateCount());
            assertEquals(2L, statement.getLargeUpdateCount());

            // get files
            statement.executeQuery(
                "get @%test_copy 'file://" + tempFolder.getCanonicalPath() + "' parallel=8");

            // Make sure that the downloaded file exists, it should be gzip compressed
            File downloaded =
                new File(tempFolder.getCanonicalPath() + File.separator + fileName + ".gz");
            assertTrue(downloaded.exists());
          }
          // unzip the new file
          Process p =
              Runtime.getRuntime()
                  .exec(
                      "gzip -d "
                          + tempFolder.getCanonicalPath()
                          + File.separator
                          + fileName
                          + ".gz");
          p.waitFor();
          File newCopy = new File(tempFolder.getCanonicalPath() + File.separator + fileName);
          // check that the get worked by uploading new file again to a different table and
          // comparing it
          // to original table
          statement.execute("create or replace table test_copy_2(c1 number, c2 number, c3 string)");

          // put copy of file
          statement.executeQuery("PUT file://" + newCopy.getPath() + " @%test_copy_2");
          // assert that the result set is empty when you subtract each table from the other
          try (ResultSet rset =
              statement.executeQuery(
                  "select * from @%test_copy minus select * from @%test_copy_2")) {
            assertFalse(rset.next());
          }
          try (ResultSet rset =
              statement.executeQuery(
                  "select * from @%test_copy_2 minus select * from @%test_copy")) {
            assertFalse(rset.next());
          }
        } finally {
          statement.execute("drop table if exists test_copy");
          statement.execute("drop table if exists test_copy_2");
        }
      }
    }
  }

  /**
   * Tests that resultsets that have been closed are not added to the set of openResultSets.
   *
   * @throws SQLException
   */
  @Test
  public void testExecuteOpenResultSets() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (int i = 0; i < 10; i++) {
        statement.execute("select 1");
        statement.getResultSet();
      }

      assertEquals(9, statement.unwrap(SnowflakeStatementV1.class).getOpenResultSets().size());
    }

    try (Statement statement = connection.createStatement()) {
      for (int i = 0; i < 10; i++) {
        statement.execute("select 1");
        ResultSet resultSet = statement.getResultSet();
        resultSet.close();
      }

      assertEquals(0, statement.unwrap(SnowflakeStatementV1.class).getOpenResultSets().size());
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testPreparedStatementLogging() throws SQLException {
    try (Connection con = getConnection();
        Statement stmt = con.createStatement()) {
      try {
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
        sfSession.setPreparedStatementLogging(true);

        stmt.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1");
        stmt.executeQuery(
            "create or replace table mytab(cola int, colb int, colc int, cold int, cole int"
                + ", colf int, colg int, colh int)");
        PreparedStatement pstatement =
            con.prepareStatement(
                "INSERT INTO mytab(cola, colb, colc, cold, cole, colf, colg, colh) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        for (int i = 1; i <= 1001; i++) {
          pstatement.setInt(1, i);
          pstatement.setInt(2, i);
          pstatement.setInt(3, i);
          pstatement.setInt(4, i);
          pstatement.setInt(5, i);
          pstatement.setInt(6, i);
          pstatement.setInt(7, i);
          pstatement.setInt(8, i);
          pstatement.addBatch();
        }

        Map<String, ParameterBindingDTO> bindings =
            pstatement.unwrap(SnowflakePreparedStatementV1.class).getBatchParameterBindings();
        assertTrue(bindings.size() > 0);
        int bindValues = BindUploader.arrayBindValueCount(bindings);
        assertEquals(8008, bindValues);
        pstatement.executeBatch();
      } finally {
        stmt.execute("drop table if exists mytab");
      }
    }
  }

  @Test // SNOW-647217
  public void testSchemaWith255CharactersDoesNotCauseException() throws SQLException {
    String schemaName =
        TestUtil.GENERATED_SCHEMA_PREFIX
            + SnowflakeUtil.randomAlphaNumeric(255 - TestUtil.GENERATED_SCHEMA_PREFIX.length());
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("create schema " + schemaName);
      stmt.execute("use schema " + schemaName);
      stmt.execute("drop schema " + schemaName);
    }
  }

  /** Added in > 3.14.4 */
  @Test
  public void testQueryIdIsSetOnFailedQueryExecute() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      assertNull(stmt.unwrap(SnowflakeStatement.class).getQueryID());
      try {
        stmt.execute("use database not_existing_database");
        fail("Statement should fail with exception");
      } catch (SnowflakeSQLException e) {
        String queryID = stmt.unwrap(SnowflakeStatement.class).getQueryID();
        TestUtil.assertValidQueryId(queryID);
        assertEquals(queryID, e.getQueryId());
      }
    }
  }

  /** Added in > 3.14.4 */
  @Test
  public void testQueryIdIsSetOnFailedExecuteUpdate() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      assertNull(stmt.unwrap(SnowflakeStatement.class).getQueryID());
      try {
        stmt.executeUpdate("update not_existing_table set a = 1 where id = 42");
        fail("Statement should fail with exception");
      } catch (SnowflakeSQLException e) {
        String queryID = stmt.unwrap(SnowflakeStatement.class).getQueryID();
        TestUtil.assertValidQueryId(queryID);
        assertEquals(queryID, e.getQueryId());
      }
    }
  }

  /** Added in > 3.14.4 */
  @Test
  public void testQueryIdIsSetOnFailedExecuteQuery() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      assertNull(stmt.unwrap(SnowflakeStatement.class).getQueryID());
      try {
        stmt.executeQuery("select * from not_existing_table");
        fail("Statement should fail with exception");
      } catch (SnowflakeSQLException e) {
        String queryID = stmt.unwrap(SnowflakeStatement.class).getQueryID();
        TestUtil.assertValidQueryId(queryID);
        assertEquals(queryID, e.getQueryId());
      }
    }
  }
}
