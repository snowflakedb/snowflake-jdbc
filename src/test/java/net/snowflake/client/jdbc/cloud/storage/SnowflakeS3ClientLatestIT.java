/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.Assert.*;

import com.amazonaws.ClientConfiguration;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.*;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.junit.Test;

public class SnowflakeS3ClientLatestIT extends BaseJDBCTest {

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testS3Client256Encryption() throws SQLException {
    try (Connection connection = getConnection("s3testaccount")) {
      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      String putCommand = "put file:///dummy/path/file1.gz @~";
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
      // create encMat with encryption keysize 256 for master key
      RemoteStoreFileEncryptionMaterial content =
          new RemoteStoreFileEncryptionMaterial(
              "LHMTKHLETLKHPSTADDGAESLFKREYGHFHGHGSDHJKLMH", "123456", 123L);
      StageInfo info = sfAgent.getStageInfo();
      ClientConfiguration config = new ClientConfiguration();
      // AmazonS3EncryptionClient builder will create the client
      SnowflakeS3Client client =
          new SnowflakeS3Client(
              info.getCredentials(),
              config,
              content,
              info.getProxyProperties(),
              info.getRegion(),
              info.getEndPoint(),
              info.getIsClientSideEncrypted(),
              sfSession,
              info.getUseS3RegionalUrl());
      assertEquals(256, client.getEncryptionKeySize());
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testS3ConnectionWithProxyEnvVariablesSet() throws SQLException {
    Connection connection = null;
    SnowflakeUtil.systemSetEnv("HTTPS_PROXY", "myproxy:8080");
    String testStageName = "s3TestStage";
    try {
      connection = getConnection("s3testaccount");
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("select 1");
      assertTrue(resultSet.next());

      statement.execute("create or replace stage " + testStageName);
      resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @" + testStageName);
      while (resultSet.next()) {
        assertEquals("UPLOADED", resultSet.getString("status"));
      }
    } finally {
      if (connection != null) {
        connection.createStatement().execute("DROP STAGE if exists " + testStageName);
        connection.close();
      }
    }
    SnowflakeUtil.systemUnsetEnv("HTTPS_PROXY");
  }
}
