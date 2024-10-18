/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryOthers.class)
public class GitRepositoryDownloadLatestIT extends BaseJDBCTest {

  /**
   * Test needs to set up git integration which is not available in GH Action tests and needs
   * accountadmin role. Added in > 3.19.0
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void shouldDownloadFileAndStreamFromGitRepository() throws Exception {
    try (Connection connection = getConnection()) {
      prepareJdbcRepoInSnowflake(connection);

      String stageName =
          String.format("@%s.%s.JDBC", connection.getCatalog(), connection.getSchema());
      String fileName = ".pre-commit-config.yaml";
      String filePathInGitRepo = "branches/master/" + fileName;

      List<String> fetchedFileContent =
          getContentFromFile(connection, stageName, filePathInGitRepo, fileName);

      List<String> fetchedStreamContent =
          getContentFromStream(connection, stageName, filePathInGitRepo);

      assertFalse("File content cannot be empty", fetchedFileContent.isEmpty());
      assertFalse("Stream content cannot be empty", fetchedStreamContent.isEmpty());
      assertEquals(fetchedFileContent, fetchedStreamContent);
    }
  }

  private static void prepareJdbcRepoInSnowflake(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("use role accountadmin");
      statement.execute(
          "CREATE OR REPLACE API INTEGRATION gh_integration\n"
              + "  API_PROVIDER = git_https_api\n"
              + "  API_ALLOWED_PREFIXES = ('https://github.com/snowflakedb/snowflake-jdbc.git')\n"
              + "  ENABLED = TRUE;");
      statement.execute(
          "CREATE OR REPLACE GIT REPOSITORY jdbc\n"
              + "ORIGIN = 'https://github.com/snowflakedb/snowflake-jdbc.git'\n"
              + "API_INTEGRATION = gh_integration;");
    }
  }

  private static List<String> getContentFromFile(
      Connection connection, String stageName, String filePathInGitRepo, String fileName)
      throws IOException, SQLException {
    Path tempDir = Files.createTempDirectory("git");
    String stagePath = stageName + "/" + filePathInGitRepo;
    Path downloadedFile = tempDir.resolve(fileName);
    String command = String.format("GET '%s' '%s'", stagePath, tempDir.toUri());

    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(command); ) {
      // then
      assertTrue("has result", rs.next());
      return Files.readAllLines(downloadedFile);
    } finally {
      Files.delete(downloadedFile);
      Files.delete(tempDir);
    }
  }

  private static List<String> getContentFromStream(
      Connection connection, String stageName, String filePathInGitRepo)
      throws SQLException, IOException {
    SnowflakeConnection unwrap = connection.unwrap(SnowflakeConnection.class);
    try (InputStream inputStream = unwrap.downloadStream(stageName, filePathInGitRepo, false)) {
      return IOUtils.readLines(inputStream, StandardCharsets.UTF_8);
    }
  }
}
