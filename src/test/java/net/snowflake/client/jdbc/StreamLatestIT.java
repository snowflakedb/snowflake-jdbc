package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Stream API tests for the latest JDBC driver. This doesn't work for the oldest supported driver.
 * Revisit this tests whenever bumping up the oldest supported driver to examine if the tests still
 * is not applicable. If it is applicable, move tests to StreamIT so that both the latest and oldest
 * supported driver run the tests.
 */
@Tag(TestTags.OTHERS)
public class StreamLatestIT extends BaseJDBCTest {

  @TempDir private File tmpFolder;

  /**
   * Test Upload Stream with atypical stage names
   *
   * @throws Throwable if any error occurs.
   */
  @Test
  public void testUnusualStageName() throws Throwable {
    String ret = null;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {

      try {
        statement.execute("CREATE or replace TABLE \"ice cream (nice)\" (types STRING)");

        FileBackedOutputStream outputStream = new FileBackedOutputStream(1000000);
        outputStream.write("hello".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();

        // upload the data to user stage under testUploadStream with name hello.txt
        connection
            .unwrap(SnowflakeConnection.class)
            .uploadStream(
                "'@%\"ice cream (nice)\"'",
                null, outputStream.asByteSource().openStream(), "hello.txt", false);

        // select from the file to make sure the data is uploaded
        try (ResultSet rset = statement.executeQuery("SELECT $1 FROM '@%\"ice cream (nice)\"/'")) {
          ret = null;

          while (rset.next()) {
            ret = rset.getString(1);
          }
          assertEquals("hello", ret, "Unexpected string value: " + ret + " expect: hello");
        }
        statement.execute("CREATE or replace TABLE \"ice cream (nice)\" (types STRING)");

        // upload the data to user stage under testUploadStream with name hello.txt
        connection
            .unwrap(SnowflakeConnection.class)
            .uploadStream(
                "$$@%\"ice cream (nice)\"$$",
                null, outputStream.asByteSource().openStream(), "hello.txt", false);

        // select from the file to make sure the data is uploaded
        try (ResultSet rset =
            statement.executeQuery("SELECT $1 FROM $$@%\"ice cream (nice)\"/$$")) {

          ret = null;

          while (rset.next()) {
            ret = rset.getString(1);
          }
          assertEquals("hello", ret, "Unexpected string value: " + ret + " expect: hello");
        }
      } finally {
        statement.execute("DROP TABLE IF EXISTS \"ice cream (nice)\"");
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDownloadToStreamBlobNotFoundGCS() throws SQLException {
    final String DEST_PREFIX = TEST_UUID + "/testUploadStream";
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);

    try (Connection connection = getConnection("gcpaccount", paramProperties);
        Statement statement = connection.createStatement()) {

      try {
        SQLException ex =
            assertThrows(
                SQLException.class,
                () ->
                    connection
                        .unwrap(SnowflakeConnection.class)
                        .downloadStream("~", DEST_PREFIX + "/abc.gz", true));
        assertTrue(
            ex.getMessage().contains("File not found"),
            "Wrong exception message: " + ex.getMessage());
      } finally {
        statement.execute("rm @~/" + DEST_PREFIX);
      }
    }
  }

  @Test
  @Disabled
  public void testDownloadToStreamGCSPresignedUrl() throws SQLException, IOException {
    final String DEST_PREFIX = "testUploadStream";

    try (Connection connection = getConnection("gcpaccount");
        Statement statement = connection.createStatement()) {
      statement.execute("create or replace stage testgcpstage");
      try (ResultSet rset =
          statement.executeQuery(
              "PUT file://"
                  + getFullPathFileInResource(TEST_DATA_FILE)
                  + " @testgcpstage/"
                  + DEST_PREFIX)) {
        assertTrue(rset.next());
        assertEquals("UPLOADED", rset.getString(7), "Error message:" + rset.getString(8));

        InputStream out =
            connection
                .unwrap(SnowflakeConnection.class)
                .downloadStream("@testgcpstage", DEST_PREFIX + "/" + TEST_DATA_FILE + ".gz", true);
        StringWriter writer = new StringWriter();
        IOUtils.copy(out, writer, "UTF-8");
        String output = writer.toString();
        // the first 2 characters
        assertEquals("1|", output.substring(0, 2));

        // the number of lines
        String[] lines = output.split("\n");
        assertEquals(28, lines.length);
      }
      statement.execute("rm @~/" + DEST_PREFIX);
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDownloadToStreamGCS() throws SQLException, IOException {
    final String DEST_PREFIX = TEST_UUID + "/testUploadStream";
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);

    try (Connection connection = getConnection("gcpaccount", paramProperties);
        Statement statement = connection.createStatement();
        ResultSet rset =
            statement.executeQuery(
                "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @~/" + DEST_PREFIX)) {
      try {
        assertTrue(rset.next());
        assertEquals("UPLOADED", rset.getString(7));

        InputStream out =
            connection
                .unwrap(SnowflakeConnection.class)
                .downloadStream("~", DEST_PREFIX + "/" + TEST_DATA_FILE + ".gz", true);
        StringWriter writer = new StringWriter();
        IOUtils.copy(out, writer, "UTF-8");
        String output = writer.toString();
        // the first 2 characters
        assertEquals("1|", output.substring(0, 2));

        // the number of lines
        String[] lines = output.split("\n");
        assertEquals(28, lines.length);
      } finally {
        statement.execute("rm @~/" + DEST_PREFIX);
      }
    }
  }

  @Test
  public void testSpecialCharactersInFileName() throws SQLException, IOException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        // Create a temporary file with special characters in the name and write to it
        File specialCharFile = new File(tmpFolder, "(special char@).txt");
        specialCharFile.createNewFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(specialCharFile))) {
          bw.write("Creating test file for downloadStream test");
        }

        String sourceFilePath = specialCharFile.getCanonicalPath();
        String sourcePathEscaped;
        if (System.getProperty("file.separator").equals("\\")) {
          // windows separator needs to be escaped because of quotes
          sourcePathEscaped = sourceFilePath.replace("\\", "\\\\");
        } else {
          sourcePathEscaped = sourceFilePath;
        }

        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE downloadStream_stage");
        statement.execute(
            "PUT 'file://" + sourcePathEscaped + "' @~/downloadStream_stage auto_compress=false");

        // download file stream
        try (InputStream out =
            connection
                .unwrap(SnowflakeConnection.class)
                .downloadStream("~", "/downloadStream_stage/" + specialCharFile.getName(), false)) {

          // Read file stream and check the result
          StringWriter writer = new StringWriter();
          IOUtils.copy(out, writer, "UTF-8");
          String output = writer.toString();
          assertEquals("Creating test file for downloadStream test", output);
        }
      } finally {
        statement.execute("DROP STAGE IF EXISTS downloadStream_stage");
      }
    }
  }

  /** Added > 3.21.0. Fixed regression introduced in 3.19.1 */
  @Test
  public void shouldDownloadStreamInDeterministicWay() throws Exception {
    try (Connection conn = getConnection();
        Statement stat = conn.createStatement()) {
      String randomStage = "test" + UUID.randomUUID().toString().replaceAll("-", "");
      try {
        stat.execute("CREATE OR REPLACE STAGE " + randomStage);
        String randomDir = UUID.randomUUID().toString();
        String sourceFilePathWithoutExtension = getFullPathFileInResource("test_file");
        String sourceFilePathWithExtension = getFullPathFileInResource("test_file.csv");
        String stageDest = String.format("@%s/%s", randomStage, randomDir);
        putFile(stat, sourceFilePathWithExtension, stageDest, false);
        putFile(stat, sourceFilePathWithoutExtension, stageDest, false);
        putFile(stat, sourceFilePathWithExtension, stageDest, true);
        putFile(stat, sourceFilePathWithoutExtension, stageDest, true);
        expectsFilesOnStage(stat, stageDest, 4);
        String stageName = "@" + randomStage;
        downloadStreamExpectingContent(
            conn, stageName, randomDir + "/test_file.gz", true, "I am a file without extension");
        downloadStreamExpectingContent(
            conn, stageName, randomDir + "/test_file.csv.gz", true, "I am a file with extension");
        downloadStreamExpectingContent(
            conn, stageName, randomDir + "/test_file", false, "I am a file without extension");
        downloadStreamExpectingContent(
            conn, stageName, randomDir + "/test_file.csv", false, "I am a file with extension");
      } finally {
        stat.execute("DROP STAGE IF EXISTS " + randomStage);
      }
    }
  }

  private static void downloadStreamExpectingContent(
      Connection conn,
      String stageName,
      String fileName,
      boolean decompress,
      String expectedFileContent)
      throws IOException, SQLException {
    try (InputStream inputStream =
            conn.unwrap(SnowflakeConnectionV1.class)
                .downloadStream(stageName, fileName, decompress);
        InputStreamReader isr = new InputStreamReader(inputStream);
        BufferedReader br = new BufferedReader(isr)) {
      String content = br.lines().collect(Collectors.joining("\n"));
      assertEquals(expectedFileContent, content);
    }
  }

  private static void expectsFilesOnStage(Statement stmt, String stageDest, int expectCount)
      throws SQLException {
    int filesInStageDir = 0;
    try (ResultSet rs = stmt.executeQuery("LIST " + stageDest)) {
      while (rs.next()) {
        ++filesInStageDir;
      }
    }
    assertEquals(expectCount, filesInStageDir);
  }

  private static boolean putFile(
      Statement stmt, String localFileName, String stageDest, boolean autoCompress)
      throws SQLException {
    return stmt.execute(
        String.format(
            "PUT file://%s %s AUTO_COMPRESS=%s",
            localFileName, stageDest, String.valueOf(autoCompress).toUpperCase()));
  }
}
