/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.AbstractDriverIT.getConnection;

import java.sql.Connection;
import java.sql.SQLException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import org.junit.Assert;
import org.junit.Test;

/** Tests for SnowflakeFileTransferAgent that require an active connection */
public class FileUploaderLatestIT extends FileUploaderSessionlessTest {

  /**
   * This tests that getStageInfo(JsonNode, session) reflects the boolean value of UseS3RegionalUrl
   * that has been set via the session.
   *
   * @throws SQLException
   */
  @Test
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
}
