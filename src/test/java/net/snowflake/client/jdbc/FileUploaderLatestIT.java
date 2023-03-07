/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
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
      connection = getConnection("gcpaccount");
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
      connection = getConnection("gcpaccount");
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
      connection = getConnection("gcpaccount");
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
    } finally {
      if (connection != null) {
        connection.createStatement().execute("DROP STAGE if exists " + OBJ_META_STAGE);
        connection.close();
      }
    }
  }
}
