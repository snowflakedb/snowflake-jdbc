/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Stream API tests for the latest JDBC driver. This doesn't work for the oldest supported driver.
 * Revisit this tests whenever bumping up the oldest supported driver to examine if the tests still
 * is not applicable. If it is applicable, move tests to StreamIT so that both the latest and oldest
 * supported driver run the tests.
 */
@Category(TestCategoryOthers.class)
public class StreamLatestIT extends BaseJDBCTest {

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

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
          assertEquals("Unexpected string value: " + ret + " expect: hello", "hello", ret);
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
          assertEquals("Unexpected string value: " + ret + " expect: hello", "hello", ret);
        }
      } finally {
        statement.execute("DROP TABLE IF EXISTS \"ice cream (nice)\"");
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testDownloadToStreamBlobNotFoundGCS() throws SQLException {
    final String DEST_PREFIX = TEST_UUID + "/testUploadStream";
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);

    try (Connection connection = getConnection("gcpaccount", paramProperties);
        Statement statement = connection.createStatement()) {

      try {
        connection
            .unwrap(SnowflakeConnection.class)
            .downloadStream("~", DEST_PREFIX + "/abc.gz", true);
        fail("should throw a storage provider exception for blob not found");
      } catch (Exception ex) {
        assertTrue(ex instanceof SQLException);
        assertTrue(
            "Wrong exception message: " + ex.getMessage(),
            ex.getMessage().contains("File not found"));
      } finally {
        statement.execute("rm @~/" + DEST_PREFIX);
      }
    }
  }

  @Test
  @Ignore
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
        assertEquals("Error message:" + rset.getString(8), "UPLOADED", rset.getString(7));

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
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
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
        File specialCharFile = tmpFolder.newFile("(special char@).txt");
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
}
