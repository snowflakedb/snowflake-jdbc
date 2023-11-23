/*
 * Copyright (c) 2022 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.ErrorCode.ROW_DOES_NOT_EXIST;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import java.io.File;
import java.net.URL;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryStatement;
import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.bind.BindUploader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Statement integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the oldest supported driver to examine
 * if the tests still is not applicable. If it is applicable, move tests to StatementIT so that both
 * the latest and oldest supported driver run the tests.
 */
@Category(TestCategoryStatement.class)
public class StatementLatestIT extends BaseJDBCTest {
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
  public void testExecuteCreateAndDrop() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

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

    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testCopyAndUpload() throws Exception {

    Connection connection = null;
    Statement statement = null;
    File tempFolder = tmpFolder.newFolder("test_downloads_folder");
    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      String fileName = "test_copy.csv";
      URL resource = StatementIT.class.getResource(fileName);

      connection = getConnection(accounts.get(i));
      statement = connection.createStatement();

      statement.execute("create or replace table test_copy(c1 number, c2 number, c3 string)");
      assertEquals(0, statement.getUpdateCount());
      assertEquals(0, statement.getLargeUpdateCount());

      String path = resource.getFile();

      // put files
      ResultSet rset = statement.executeQuery("PUT file://" + path + " @%test_copy");
      try {
        rset.getString(1);
        fail("Should raise No row found exception, because no next() is called.");
      } catch (SQLException ex) {
        assertThat(
            "No row found error", ex.getErrorCode(), equalTo(ROW_DOES_NOT_EXIST.getMessageCode()));
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
      File downloaded = new File(tempFolder.getCanonicalPath() + File.separator + fileName + ".gz");
      assert (downloaded.exists());

      // unzip the new file
      Process p =
          Runtime.getRuntime()
              .exec("gzip -d " + tempFolder.getCanonicalPath() + File.separator + fileName + ".gz");
      p.waitFor();
      File newCopy = new File(tempFolder.getCanonicalPath() + File.separator + fileName);

      // check that the get worked by uploading new file again to a different table and comparing it
      // to original table
      statement.execute("create or replace table test_copy_2(c1 number, c2 number, c3 string)");

      // put copy of file
      rset = statement.executeQuery("PUT file://" + newCopy.getPath() + " @%test_copy_2");
      // assert that the result set is empty when you subtract each table from the other
      rset = statement.executeQuery("select * from @%test_copy minus select * from @%test_copy_2");
      assertFalse(rset.next());
      rset = statement.executeQuery("select * from @%test_copy_2 minus select * from @%test_copy");
      assertFalse(rset.next());

      statement.execute("drop table if exists test_copy");
      statement.execute("drop table if exists test_copy_2");
    }

    statement.close();
    connection.close();
  }

  /**
   * Tests that resultsets that have been closed are not added to the set of openResultSets.
   *
   * @throws SQLException
   */
  @Test
  public void testExecuteOpenResultSets() throws SQLException {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    ResultSet resultSet;

    for (int i = 0; i < 10; i++) {
      statement.execute("select 1");
      statement.getResultSet();
    }

    assertEquals(9, statement.unwrap(SnowflakeStatementV1.class).getOpenResultSets().size());
    statement.close();

    statement = con.createStatement();
    for (int i = 0; i < 10; i++) {
      statement.execute("select 1");
      resultSet = statement.getResultSet();
      resultSet.close();
    }

    assertEquals(0, statement.unwrap(SnowflakeStatementV1.class).getOpenResultSets().size());

    statement.close();
    con.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPreparedStatementLogging() throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = con.createStatement()) {
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

        stmt.execute("drop table if exists mytab");
      }
    }
  }

  @Test // SNOW-647217
  public void testSchemaWith255CharactersDoesNotCauseException() throws SQLException {
    String schemaName = "a" + SnowflakeUtil.randomAlphaNumeric(254);
    try (Connection con = getConnection()) {
      try (Statement stmt = con.createStatement()) {
        stmt.execute("create schema " + schemaName);
        stmt.execute("use schema " + schemaName);
        stmt.execute("drop schema " + schemaName);
      }
    }
  }
}
