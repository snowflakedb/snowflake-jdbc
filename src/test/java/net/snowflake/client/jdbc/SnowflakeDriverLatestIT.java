/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * General JDBC tests for the latest JDBC driver. This doesn't work for the oldest supported driver.
 * Revisit this tests whenever bumping up the oldest supported driver to examine if the tests still
 * is not applicable. If it is applicable, move tests to SnowflakeDriverIT so that both the latest
 * and oldest supported driver run the tests.
 */
@Category(TestCategoryOthers.class)
public class SnowflakeDriverLatestIT extends BaseJDBCTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  public String testStageName =
      String.format("test_stage_%s", UUID.randomUUID().toString()).replaceAll("-", "_");

  // Check whether the two file's content are logically identical
  private boolean isFileContentEqual(
      String fileFullPath1, boolean compressedFile1, String fileFullPath2, boolean compressedFile2)
      throws Throwable {
    InputStream inputStream1 = new FileInputStream(fileFullPath1);
    InputStream inputStream2 = new FileInputStream(fileFullPath2);

    try {
      if (compressedFile1) {
        inputStream1 = new GZIPInputStream(inputStream1);
      }
      if (compressedFile2) {
        inputStream2 = new GZIPInputStream(inputStream2);
      }
      return Arrays.equals(IOUtils.toByteArray(inputStream1), IOUtils.toByteArray(inputStream2));
    } finally {
      inputStream1.close();
      inputStream2.close();
    }
  }

  @Test
  public void testGetSessionID() throws Throwable {
    Connection con = getConnection();
    String sessionID = con.unwrap(SnowflakeConnection.class).getSessionID();
    Statement statement = con.createStatement();
    ResultSet rset = statement.executeQuery("select current_session()");
    rset.next();
    assertEquals(sessionID, rset.getString(1));
  }

  /** Test API for Spark connector for FileTransferMetadata */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGCPFileTransferMetadataWithOneFile() throws Throwable {
    Connection connection = null;
    File destFolder = tmpFolder.newFolder();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    try {
      connection = getConnection("gcpaccount");
      Statement statement = connection.createStatement();

      // create a stage to put the file in
      statement.execute("CREATE OR REPLACE STAGE " + testStageName);

      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

      // Test put file with internal compression
      String putCommand1 = "put file:///dummy/path/file1.gz @" + testStageName;
      SnowflakeFileTransferAgent sfAgent1 =
          new SnowflakeFileTransferAgent(putCommand1, sfSession, new SFStatement(sfSession));
      List<SnowflakeFileTransferMetadata> metadatas1 = sfAgent1.getFileTransferMetadatas();

      String srcPath1 = getFullPathFileInResource(TEST_DATA_FILE);
      for (SnowflakeFileTransferMetadata oneMetadata : metadatas1) {
        InputStream inputStream = new FileInputStream(srcPath1);

        assert (oneMetadata.isForOneFile());
        SnowflakeFileTransferAgent.uploadWithoutConnection(
            SnowflakeFileTransferConfig.Builder.newInstance()
                .setSnowflakeFileTransferMetadata(oneMetadata)
                .setUploadStream(inputStream)
                .setRequireCompress(true)
                .setNetworkTimeoutInMilli(0)
                .setOcspMode(OCSPMode.FAIL_OPEN)
                .build());
      }

      // Test Put file with external compression
      String putCommand2 = "put file:///dummy/path/file2.gz @" + testStageName;
      SnowflakeFileTransferAgent sfAgent2 =
          new SnowflakeFileTransferAgent(putCommand2, sfSession, new SFStatement(sfSession));
      List<SnowflakeFileTransferMetadata> metadatas2 = sfAgent2.getFileTransferMetadatas();

      String srcPath2 = getFullPathFileInResource(TEST_DATA_FILE_2);
      for (SnowflakeFileTransferMetadata oneMetadata : metadatas2) {
        String gzfilePath = destFolderCanonicalPath + "/tmp_compress.gz";
        Process p =
            Runtime.getRuntime()
                .exec("cp -fr " + srcPath2 + " " + destFolderCanonicalPath + "/tmp_compress");
        p.waitFor();
        p = Runtime.getRuntime().exec("gzip " + destFolderCanonicalPath + "/tmp_compress");
        p.waitFor();

        InputStream gzInputStream = new FileInputStream(gzfilePath);
        assert (oneMetadata.isForOneFile());
        SnowflakeFileTransferAgent.uploadWithoutConnection(
            SnowflakeFileTransferConfig.Builder.newInstance()
                .setSnowflakeFileTransferMetadata(oneMetadata)
                .setUploadStream(gzInputStream)
                .setRequireCompress(false)
                .setNetworkTimeoutInMilli(0)
                .setOcspMode(OCSPMode.FAIL_OPEN)
                .build());
      }

      // Download two files and verify their content.
      assertTrue(
          "Failed to get files",
          statement.execute(
              "GET @" + testStageName + " 'file://" + destFolderCanonicalPath + "/' parallel=8"));

      // Make sure that the downloaded files are EQUAL,
      // they should be gzip compressed
      assert (isFileContentEqual(srcPath1, false, destFolderCanonicalPath + "/file1.gz", true));
      assert (isFileContentEqual(srcPath2, false, destFolderCanonicalPath + "/file2.gz", true));
    } finally {
      if (connection != null) {
        connection.createStatement().execute("DROP STAGE if exists " + testStageName);
        connection.close();
      }
    }
  }

  /** Test API for Kafka connector for FileTransferMetadata */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testAzureS3FileTransferMetadataWithOneFile() throws Throwable {
    Connection connection = null;
    File destFolder = tmpFolder.newFolder();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();

    List<String> supportedAaccounts = Arrays.asList("s3testaccount", "azureaccount");
    for (String accountName : supportedAaccounts) {
      try {
        connection = getConnection(accountName);
        Statement statement = connection.createStatement();

        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE " + testStageName);

        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

        // Test put file with internal compression
        String putCommand1 = "put file:///dummy/path/file1.gz @" + testStageName;
        SnowflakeFileTransferAgent sfAgent1 =
            new SnowflakeFileTransferAgent(putCommand1, sfSession, new SFStatement(sfSession));
        List<SnowflakeFileTransferMetadata> metadatas1 = sfAgent1.getFileTransferMetadatas();

        String srcPath1 = getFullPathFileInResource(TEST_DATA_FILE);
        for (SnowflakeFileTransferMetadata oneMetadata : metadatas1) {
          InputStream inputStream = new FileInputStream(srcPath1);

          SnowflakeFileTransferAgent.uploadWithoutConnection(
              SnowflakeFileTransferConfig.Builder.newInstance()
                  .setSnowflakeFileTransferMetadata(oneMetadata)
                  .setUploadStream(inputStream)
                  .setRequireCompress(true)
                  .setNetworkTimeoutInMilli(0)
                  .setOcspMode(OCSPMode.FAIL_OPEN)
                  .setSFSession(sfSession)
                  .setCommand(putCommand1)
                  .build());
        }

        // Test Put file with external compression
        String putCommand2 = "put file:///dummy/path/file2.gz @" + testStageName;
        SnowflakeFileTransferAgent sfAgent2 =
            new SnowflakeFileTransferAgent(putCommand2, sfSession, new SFStatement(sfSession));
        List<SnowflakeFileTransferMetadata> metadatas2 = sfAgent2.getFileTransferMetadatas();

        String srcPath2 = getFullPathFileInResource(TEST_DATA_FILE_2);
        for (SnowflakeFileTransferMetadata oneMetadata : metadatas2) {
          String gzfilePath = destFolderCanonicalPath + "/tmp_compress.gz";
          Process p =
              Runtime.getRuntime()
                  .exec("cp -fr " + srcPath2 + " " + destFolderCanonicalPath + "/tmp_compress");
          p.waitFor();
          p = Runtime.getRuntime().exec("gzip " + destFolderCanonicalPath + "/tmp_compress");
          p.waitFor();

          InputStream gzInputStream = new FileInputStream(gzfilePath);

          SnowflakeFileTransferAgent.uploadWithoutConnection(
              SnowflakeFileTransferConfig.Builder.newInstance()
                  .setSnowflakeFileTransferMetadata(oneMetadata)
                  .setUploadStream(gzInputStream)
                  .setRequireCompress(false)
                  .setNetworkTimeoutInMilli(0)
                  .setOcspMode(OCSPMode.FAIL_OPEN)
                  .setSFSession(sfSession)
                  .setCommand(putCommand2)
                  .build());
        }

        // Download two files and verify their content.
        assertTrue(
            "Failed to get files",
            statement.execute(
                "GET @" + testStageName + " 'file://" + destFolderCanonicalPath + "/' parallel=8"));

        // Make sure that the downloaded files are EQUAL,
        // they should be gzip compressed
        assert (isFileContentEqual(srcPath1, false, destFolderCanonicalPath + "/file1.gz", true));
        assert (isFileContentEqual(srcPath2, false, destFolderCanonicalPath + "/file2.gz", true));
      } finally {
        if (connection != null) {
          connection.createStatement().execute("DROP STAGE if exists " + testStageName);
          connection.close();
        }
      }
    }
  }

  /** Negative test for FileTransferMetadata. It is only supported for PUT. */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGCPFileTransferMetadataNetativeOnlySupportPut() throws Throwable {
    Connection connection = null;
    int expectExceptionCount = 1;
    int actualExceptionCount = -1;
    try {
      connection = getConnection("gcpaccount");
      Statement statement = connection.createStatement();

      // create a stage to put the file in
      statement.execute("CREATE OR REPLACE STAGE " + testStageName);

      // Put one file to the stage
      String srcPath = getFullPathFileInResource(TEST_DATA_FILE);
      statement.execute("put file://" + srcPath + " @" + testStageName);

      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

      File destFolder = tmpFolder.newFolder();
      String destFolderCanonicalPath = destFolder.getCanonicalPath();

      String getCommand = "get @" + testStageName + " file://" + destFolderCanonicalPath;

      // The GET can be executed in normal way.
      statement.execute(getCommand);

      // Start negative test for GET.
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(getCommand, sfSession, new SFStatement(sfSession));

      // Below function call should fail.
      actualExceptionCount = 0;
      sfAgent.getFileTransferMetadatas();
      fail("Above function should raise exception for GET");
    } catch (Exception ex) {
      System.out.println("Negative test to hit expected exception: " + ex.getMessage());
      actualExceptionCount++;
    } finally {
      if (connection != null) {
        connection.createStatement().execute("DROP STAGE if exists " + testStageName);
        connection.close();
      }
    }
    assertEquals(expectExceptionCount, actualExceptionCount);
  }

  @Test
  public void testGetPropertyInfo() throws SQLException {
    // Test with blank URL and no properties. ServerURL is needed.
    String url = "";
    Properties props = new Properties();
    Driver driver = DriverManager.getDriver("jdbc:snowflake://snowflake.reg.local:8082");
    DriverPropertyInfo[] info = driver.getPropertyInfo(url, props);
    assertEquals(1, info.length);
    assertEquals("serverURL", info[0].name);
    assertEquals(
        "server URL in form of <protocol>://<host or domain>:<port number>/<path of resource>",
        info[0].description);

    // Test with URL that requires username and password.
    url = "jdbc:snowflake://snowflake.reg.local:8082";
    info = driver.getPropertyInfo(url, props);
    assertEquals(2, info.length);
    assertEquals("user", info[0].name);
    assertEquals("username for account", info[0].description);
    assertEquals("password", info[1].name);
    assertEquals("password for account", info[1].description);

    // Add username and try again; get password requirement back
    props.put("user", "snowman");
    props.put("password", "test");
    info = driver.getPropertyInfo(url, props);
    assertEquals(0, info.length);

    props.put("useProxy", "true");
    info = driver.getPropertyInfo(url, props);
    assertEquals(2, info.length);
    assertEquals("proxyHost", info[0].name);
    assertEquals("proxy host name", info[0].description);
    assertEquals("proxyPort", info[1].name);
    assertEquals("proxy port; should be an integer", info[1].description);

    props.put("proxyHost", "dummyHost");
    props.put("proxyPort", "dummyPort");
    info = driver.getPropertyInfo(url, props);
    assertEquals(0, info.length);

    // invalid URL still throws SQLException
    try {
      url = "snowflake.reg.local:8082";
      driver.getPropertyInfo(url, props);
    } catch (SQLException e) {
      assertEquals((int) ErrorCode.INVALID_CONNECT_STRING.getMessageCode(), e.getErrorCode());
    }
  }
}
