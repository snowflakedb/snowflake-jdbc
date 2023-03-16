/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeGCSClient;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.client.jdbc.cloud.storage.StorageProviderException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Tests for SnowflakeFileTransferAgent that require an active connection */
@Category(TestCategoryOthers.class)
public class FileUploaderLatestIT extends FileUploaderPrepIT {
  private static final String OBJ_META_STAGE = "testObjMeta";
  private ObjectMapper mapper = new ObjectMapper();
  private static final String PUT_COMMAND = "put file:///dummy/path/file2.gz @testStage";

  /**
   * This tests that getStageInfo(JsonNode, session) reflects the boolean value of UseS3RegionalUrl
   * that has been set via the session.
   *
   * @throws SQLException
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetS3StageDataWithS3Session() throws SQLException {
    Connection con = getConnection("s3testaccount");
    SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
    // Set UseRegionalS3EndpointsForPresignedURL to true in session
    sfSession.setUseRegionalS3EndpointsForPresignedURL(true);

    // Get sample stage info with session
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleS3JsonNode, sfSession);
    Assert.assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    // Assert that true value from session is reflected in StageInfo
    Assert.assertEquals(true, stageInfo.getUseS3RegionalUrl());

    // Set UseRegionalS3EndpointsForPresignedURL to false in session
    sfSession.setUseRegionalS3EndpointsForPresignedURL(false);
    stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleS3JsonNode, sfSession);
    Assert.assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    // Assert that false value from session is reflected in StageInfo
    Assert.assertEquals(false, stageInfo.getUseS3RegionalUrl());
    con.close();
  }

  /**
   * This tests that setting the value of UseS3RegionalUrl for a non-S3 account session has no
   * effect on the function getStageInfo(JsonNode, session).
   *
   * @throws SQLException
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetS3StageDataWithAzureSession() throws SQLException {
    Connection con = getConnection("azureaccount");
    SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
    // Set UseRegionalS3EndpointsForPresignedURL to true in session. This is redundant since session
    // is Azure
    sfSession.setUseRegionalS3EndpointsForPresignedURL(true);

    // Get sample stage info with session
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleAzureJsonNode, sfSession);
    Assert.assertEquals(StageInfo.StageType.AZURE, stageInfo.getStageType());
    Assert.assertEquals("EXAMPLE_LOCATION/", stageInfo.getLocation());
    // Assert that UseRegionalS3EndpointsForPresignedURL is false in StageInfo even if it was set to
    // true.
    // The value should always be false for non-S3 accounts
    Assert.assertEquals(false, stageInfo.getUseS3RegionalUrl());
    con.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetObjectMetadataWithGCS() throws Exception {
    Connection connection = null;
    try {
      Properties paramProperties = new Properties();
      paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
      connection = getConnection("gcpaccount", paramProperties);
      Statement statement = connection.createStatement();
      statement.execute("CREATE OR REPLACE STAGE " + OBJ_META_STAGE);

      String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
      String putCommand = "PUT file://" + sourceFilePath + " @" + OBJ_META_STAGE;
      statement.execute(putCommand);

      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
      StageInfo info = sfAgent.getStageInfo();
      SnowflakeGCSClient client =
          SnowflakeGCSClient.createSnowflakeGCSClient(
              info, sfAgent.getEncryptionMaterial().get(0), sfSession);

      String location = info.getLocation();
      int idx = location.indexOf('/');
      String remoteStageLocation = location.substring(0, idx);
      String path = location.substring(idx + 1) + TEST_DATA_FILE + ".gz";
      StorageObjectMetadata metadata = client.getObjectMetadata(remoteStageLocation, path);
      Assert.assertEquals("gzip", metadata.getContentEncoding());
    } finally {
      if (connection != null) {
        connection.createStatement().execute("DROP STAGE if exists " + OBJ_META_STAGE);
        connection.close();
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetObjectMetadataFileNotFoundWithGCS() throws Exception {
    Connection connection = null;
    try {
      Properties paramProperties = new Properties();
      paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
      connection = getConnection("gcpaccount", paramProperties);
      Statement statement = connection.createStatement();
      statement.execute("CREATE OR REPLACE STAGE " + OBJ_META_STAGE);

      String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
      String putCommand = "PUT file://" + sourceFilePath + " @" + OBJ_META_STAGE;
      statement.execute(putCommand);

      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
      StageInfo info = sfAgent.getStageInfo();
      SnowflakeGCSClient client =
          SnowflakeGCSClient.createSnowflakeGCSClient(
              info, sfAgent.getEncryptionMaterial().get(0), sfSession);

      String location = info.getLocation();
      int idx = location.indexOf('/');
      String remoteStageLocation = location.substring(0, idx);
      String path = location.substring(idx + 1) + "wrong_file.csv.gz";
      client.getObjectMetadata(remoteStageLocation, path);
      fail("should raise exception");
    } catch (Exception ex) {
      assertTrue(
          "Wrong type of exception. Message: " + ex.getMessage(),
          ex instanceof StorageProviderException);
      assertTrue(ex.getMessage().matches(".*Blob.*not found in bucket.*"));
    } finally {
      if (connection != null) {
        connection.createStatement().execute("DROP STAGE if exists " + OBJ_META_STAGE);
        connection.close();
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetObjectMetadataStorageExceptionWithGCS() throws Exception {
    Connection connection = null;
    try {
      Properties paramProperties = new Properties();
      paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
      connection = getConnection("gcpaccount", paramProperties);
      Statement statement = connection.createStatement();
      statement.execute("CREATE OR REPLACE STAGE " + OBJ_META_STAGE);

      String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
      String putCommand = "PUT file://" + sourceFilePath + " @" + OBJ_META_STAGE;
      statement.execute(putCommand);

      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
      StageInfo info = sfAgent.getStageInfo();
      SnowflakeGCSClient client =
          SnowflakeGCSClient.createSnowflakeGCSClient(
              info, sfAgent.getEncryptionMaterial().get(0), sfSession);

      String location = info.getLocation();
      int idx = location.indexOf('/');
      String remoteStageLocation = location.substring(0, idx);
      client.getObjectMetadata(remoteStageLocation, "");
      fail("should raise exception");
    } catch (Exception ex) {
      assertTrue(
          "Wrong type of exception. Message: " + ex.getMessage(),
          ex instanceof StorageProviderException);
      assertTrue(ex.getMessage().matches(".*Permission.*denied.*"));
    } finally {
      if (connection != null) {
        connection.createStatement().execute("DROP STAGE if exists " + OBJ_META_STAGE);
        connection.close();
      }
    }
  }

  @Test
  public void testGetFileTransferCommandType() throws SQLException {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    statement.execute("CREATE OR REPLACE STAGE testStage");
    SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
    SnowflakeFileTransferAgent sfAgent =
        new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));
    assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, sfAgent.getCommandType());
    statement.execute("drop stage if exists testStage");
    con.close();
  }

  @Test
  public void testNullCommand() throws SQLException {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    statement.execute("create or replace stage testStage");
    SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
    try {
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(null, sfSession, new SFStatement(sfSession));
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assert.assertTrue(
          err.getMessage()
              .contains("JDBC driver internal error: Missing sql for statement execution"));
    }
    statement.execute("drop stage if exists testStage");
    con.close();
  }

  @Test
  public void testCompressStreamWithGzipException() throws Exception {
    Connection con = null;
    // inject the NoSuchAlgorithmException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(new NoSuchAlgorithmException());

    try {
      con = getConnection();
      Statement statement = con.createStatement();
      statement.execute("create or replace stage testStage");
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));

      List<SnowflakeFileTransferMetadata> metadataList = sfAgent.getFileTransferMetadatas();
      SnowflakeFileTransferMetadataV1 metadata =
          (SnowflakeFileTransferMetadataV1) metadataList.get(0);

      String srcPath = getFullPathFileInResource(TEST_DATA_FILE);
      InputStream inputStream = new FileInputStream(srcPath);
      SnowflakeFileTransferAgent.uploadWithoutConnection(
          SnowflakeFileTransferConfig.Builder.newInstance()
              .setSnowflakeFileTransferMetadata(metadata)
              .setUploadStream(inputStream)
              .setRequireCompress(true)
              .setNetworkTimeoutInMilli(0)
              .setOcspMode(OCSPMode.FAIL_OPEN)
              .setSFSession(sfSession)
              .setCommand(PUT_COMMAND)
              .build());
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assert.assertTrue(
          err.getMessage()
              .contains("JDBC driver internal error: error encountered for compression"));
    } finally {
      if (con != null) {
        con.createStatement().execute("DROP STAGE if exists testStage");
        con.close();
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testCompressStreamWithGzipNoDigestException() throws Exception {
    Connection con = null;
    // inject the IOException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(new IOException());

    try {
      con = getConnection();
      Statement statement = con.createStatement();
      statement.execute("create or replace stage testStage");
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));

      List<SnowflakeFileTransferMetadata> metadataList = sfAgent.getFileTransferMetadatas();
      SnowflakeFileTransferMetadataV1 metadata =
          (SnowflakeFileTransferMetadataV1) metadataList.get(0);
      metadata.setEncryptionMaterial(null, null, null);

      String srcPath = getFullPathFileInResource(TEST_DATA_FILE);

      InputStream inputStream = new FileInputStream(srcPath);
      SnowflakeFileTransferAgent.uploadWithoutConnection(
          SnowflakeFileTransferConfig.Builder.newInstance()
              .setSnowflakeFileTransferMetadata(metadata)
              .setUploadStream(inputStream)
              .setRequireCompress(true)
              .setNetworkTimeoutInMilli(0)
              .setOcspMode(OCSPMode.FAIL_OPEN)
              .setSFSession(sfSession)
              .setCommand(PUT_COMMAND)
              .build());
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assert.assertTrue(
          err.getMessage()
              .contains("JDBC driver internal error: error encountered for compression"));
    } finally {
      if (con != null) {
        con.createStatement().execute("DROP STAGE if exists testStage");
        con.close();
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testUploadWithoutConnectionException() throws Exception {
    Connection con = null;
    // inject the IOException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(
        new Exception("Exception encountered during file upload: failed to push to remote store"));

    try {
      con = getConnection();
      Statement statement = con.createStatement();
      statement.execute("create or replace stage testStage");
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));

      List<SnowflakeFileTransferMetadata> metadataList = sfAgent.getFileTransferMetadatas();
      SnowflakeFileTransferMetadataV1 metadata =
          (SnowflakeFileTransferMetadataV1) metadataList.get(0);

      String srcPath = getFullPathFileInResource(TEST_DATA_FILE);

      InputStream inputStream = new FileInputStream(srcPath);
      SnowflakeFileTransferAgent.uploadWithoutConnection(
          SnowflakeFileTransferConfig.Builder.newInstance()
              .setSnowflakeFileTransferMetadata(metadata)
              .setUploadStream(inputStream)
              .setRequireCompress(true)
              .setNetworkTimeoutInMilli(0)
              .setOcspMode(OCSPMode.FAIL_OPEN)
              .setSFSession(sfSession)
              .setCommand(PUT_COMMAND)
              .build());
    } catch (Exception err) {
      Assert.assertTrue(
          err.getMessage()
              .contains(
                  "Exception encountered during file upload: failed to push to remote store"));
    } finally {
      if (con != null) {
        con.createStatement().execute("DROP STAGE if exists testStage");
        con.close();
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testInitFileMetadataFileNotFound() throws Exception {
    Connection con = null;
    try {
      con = getConnection();
      Statement statement = con.createStatement();
      statement.execute("create or replace stage testStage");
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));

      sfAgent.execute();
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals(200008, err.getErrorCode());
    } finally {
      if (con != null) {
        con.createStatement().execute("DROP STAGE if exists testStage");
        con.close();
      }
    }
  }

  @Test
  public void testInitFileMetadataFileIsDirectory() throws Exception {
    Connection con = null;
    try {
      con = getConnection();
      Statement statement = con.createStatement();
      statement.execute("create or replace stage testStage");
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
      String srcPath =
          getFullPathFileInResource(""); // will pull the resources directory without a file
      String command = "put file://" + srcPath + " @testStage";
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));
      sfAgent.execute();
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals(200009, err.getErrorCode());
    } finally {
      if (con != null) {
        con.createStatement().execute("DROP STAGE if exists testStage");
        con.close();
      }
    }
  }

  @Test
  public void testCompareAndSkipFilesException() throws Exception {
    Connection con = null;
    // inject the NoSuchAlgorithmException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(new NoSuchAlgorithmException());

    try {
      con = getConnection();
      Statement statement = con.createStatement();
      statement.execute("create or replace stage testStage");
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
      String command = "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @testStage";
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));

      sfAgent.execute();
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assert.assertTrue(err.getMessage().contains("Error reading:"));
    } finally {
      if (con != null) {
        con.createStatement().execute("DROP STAGE if exists testStage");
        con.close();
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testParseCommandException() throws SQLException {
    Connection con = null;
    // inject the SnowflakeSQLException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(
        new SnowflakeSQLException("invalid data"));
    try {
      con = getConnection();
      Statement statement = con.createStatement();
      statement.execute("create or replace stage testStage");
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assert.assertTrue(err.getMessage().contains("Failed to parse the locations"));
    } finally {
      if (con != null) {
        con.createStatement().execute("DROP STAGE if exists testStage");
        con.close();
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testPopulateStatusRowsWithSortOn() throws Exception {
    Connection con = null;
    try {
      con = getConnection();
      Statement statement = con.createStatement();
      statement.execute("create or replace stage testStage");
      statement.execute("set-sf-property sort on");
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

      // upload files orders_101.csv and orders_100.csv
      String command = "PUT file://" + getFullPathFileInResource("") + "/orders_10*.csv @testStage";
      SnowflakeFileTransferAgent sfAgent1 =
          new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));
      sfAgent1.execute(); // upload files

      // check that source files were sorted
      assertEquals(2, sfAgent1.statusRows.size());
      assertEquals("orders_100.csv", sfAgent1.getNextRow().get(0).toString());

      String getCommand = "GET @testStage file:///tmp";
      SnowflakeFileTransferAgent sfAgent2 =
          new SnowflakeFileTransferAgent(getCommand, sfSession, new SFStatement(sfSession));
      sfAgent2.execute();
      // check that files are sorted on download
      assertEquals(2, sfAgent2.statusRows.size());
      assertEquals("orders_100.csv.gz", sfAgent2.getNextRow().get(0).toString());
    } finally {
      if (con != null) {
        con.createStatement().execute("DROP STAGE if exists testStage");
        con.close();
      }
    }
  }

  @Test
  public void testListObjectsStorageException() throws Exception {
    Connection con = null;
    // inject the StorageProviderException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(
        new StorageProviderException(new Exception("could not list objects")));

    try {
      con = getConnection();
      Statement statement = con.createStatement();
      statement.execute("create or replace stage testStage");
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
      String command = "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @testStage";
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));

      sfAgent.execute();
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals(200016, err.getErrorCode());
      Assert.assertTrue(err.getMessage().contains("Encountered exception during listObjects"));
    } finally {
      if (con != null) {
        con.createStatement().execute("DROP STAGE if exists testStage");
        con.close();
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testUploadStreamInterruptedException() throws IOException, SQLException {
    final String DEST_PREFIX = TEST_UUID + "/testUploadStream";
    // inject the InterruptedException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(new InterruptedException());
    Connection connection = null;
    Statement statement = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

      FileBackedOutputStream outputStream = new FileBackedOutputStream(1000000);
      outputStream.write("hello".getBytes(StandardCharsets.UTF_8));
      outputStream.flush();

      // upload the data to user stage under testUploadStream with name hello.txt
      connection
          .unwrap(SnowflakeConnection.class)
          .uploadStream(
              "~", DEST_PREFIX, outputStream.asByteSource().openStream(), "hello.txt", false);

    } catch (SnowflakeSQLLoggedException err) {
      Assert.assertEquals(200003, err.getErrorCode());
    } finally {
      if (statement != null) {
        statement.execute("rm @~/" + DEST_PREFIX);
        statement.close();
      }
      closeSQLObjects(statement, connection);
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testFileTransferStageInfo() throws SQLException {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    statement.execute("CREATE OR REPLACE STAGE testStage");
    SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

    SnowflakeFileTransferAgent sfAgent =
        new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));

    StageInfo stageInfo = sfAgent.getStageInfo();
    assertEquals(sfAgent.getStageCredentials(), stageInfo.getCredentials());
    assertEquals(sfAgent.getStageLocation(), stageInfo.getLocation());

    statement.execute("drop stage if exists testStage");
    con.close();
  }

  @Test
  public void testFileTransferMappingFromSourceFile() throws SQLException {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    statement.execute("CREATE OR REPLACE STAGE testStage");
    SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

    String command = "PUT file://" + getFullPathFileInResource("") + "/orders_10*.csv @testStage";
    SnowflakeFileTransferAgent sfAgent1 =
        new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));
    sfAgent1.execute();

    SnowflakeFileTransferAgent sfAgent2 =
        new SnowflakeFileTransferAgent(
            "GET @testStage file:///tmp/", sfSession, new SFStatement(sfSession));

    assertEquals(2, sfAgent2.getSrcToMaterialsMap().size());
    assertEquals(2, sfAgent2.getSrcToPresignedUrlMap().size());

    statement.execute("drop stage if exists testStage");
    con.close();
  }

  @Test
  public void testUploadFileCallableFileNotFound() throws Exception {
    // inject the FileNotFoundException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(
        new FileNotFoundException("file does not exist"));
    Connection connection = null;
    Statement statement = null;
    try {
      connection = getConnection();

      statement = connection.createStatement();
      statement.execute("CREATE OR REPLACE STAGE testStage");
      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

      String command = "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @testStage";
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));
      sfAgent.execute();
    } catch (Exception err) {
      assertEquals(err.getCause(), instanceOf(FileNotFoundException.class));
    } finally {
      if (connection != null) {
        connection.createStatement().execute("DROP STAGE if exists testStage");
        connection.close();
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }
}
