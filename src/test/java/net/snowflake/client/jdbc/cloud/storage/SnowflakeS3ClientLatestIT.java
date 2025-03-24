package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag(TestTags.OTHERS)
public class SnowflakeS3ClientLatestIT extends BaseJDBCTest {

  @Test
  @DontRunOnGithubActions
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

  /**
   * This is a manual test to confirm that the s3 client builder doesn't read from
   * https_proxy/http_proxy environment variable.
   *
   * <p>Prerequisite: 1. Set HTTPS_PROXY/HTTP_PROXY to a proxy that won't connect i.e.
   * HTTPS_PROXY=https://myproxy:8080
   *
   * <p>2. Connect to S3 host.
   *
   * @throws SQLException
   */
  @Test
  @Disabled
  public void testS3ConnectionWithProxyEnvVariablesSet() throws SQLException {
    String testStageName = "s3TestStage";

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("select 1")) {
        assertTrue(resultSet.next());
      }
      try {
        statement.execute("create or replace stage " + testStageName);
        try (ResultSet resultSet =
            connection
                .createStatement()
                .executeQuery(
                    "PUT file://"
                        + getFullPathFileInResource(TEST_DATA_FILE)
                        + " @"
                        + testStageName)) {
          while (resultSet.next()) {
            assertEquals("UPLOADED", resultSet.getString("status"));
          }
        }
      } finally {
        statement.execute("DROP STAGE if exists " + testStageName);
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testIsClientException400Or404() throws SQLException {
    AmazonServiceException servEx = new AmazonServiceException("S3 operation failed");
    servEx.setServiceName("Amazon S3");
    servEx.setErrorMessage("Bad Request");
    servEx.setStatusCode(400);
    servEx.setErrorCode("400 Bad Request");
    servEx.setErrorType(AmazonServiceException.ErrorType.Client);

    try (Connection connection = getConnection("s3testaccount")) {
      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      String getCommand = "GET '@~/testStage' 'file://C:/temp' PARALLEL=8";
      SnowflakeFileTransferAgent agent =
          new SnowflakeFileTransferAgent(getCommand, sfSession, new SFStatement(sfSession));
      RemoteStoreFileEncryptionMaterial content =
          new RemoteStoreFileEncryptionMaterial(
              "LHMTKHLETLKHPSTADDGAESLFKREYGHFHGHGSDHJKLMH", "123456", 123L);
      StageInfo info = agent.getStageInfo();
      ClientConfiguration clientConfig = new ClientConfiguration();
      SnowflakeS3Client client =
          new SnowflakeS3Client(
              info.getCredentials(),
              clientConfig,
              content,
              info.getProxyProperties(),
              info.getRegion(),
              info.getEndPoint(),
              info.getIsClientSideEncrypted(),
              sfSession,
              info.getUseS3RegionalUrl());
      assertTrue(client.isClientException400Or404(servEx));
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testPutGetMaxRetries() throws SQLException {
    Properties props = new Properties();
    props.put("putGetMaxRetries", 1);
    try (Connection connection = getConnection("s3testaccount", props)) {
      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      String command = "GET '@~/testStage' 'file://C:/temp' PARALLEL=8";
      SnowflakeFileTransferAgent agent =
          new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));
      StageInfo info = agent.getStageInfo();
      ClientConfiguration clientConfig = new ClientConfiguration();
      RemoteStoreFileEncryptionMaterial content =
          new RemoteStoreFileEncryptionMaterial(
              "LHMTKHLETLKHPSTADDGAESLFKREYGHFHGHGSDHJKLMH", "123456", 123L);
      SnowflakeS3Client client =
          new SnowflakeS3Client(
              info.getCredentials(),
              clientConfig,
              content,
              info.getProxyProperties(),
              info.getRegion(),
              info.getEndPoint(),
              info.getIsClientSideEncrypted(),
              sfSession,
              info.getUseS3RegionalUrl());
      SnowflakeS3Client spy = Mockito.spy(client);

      // Should retry one time, then throw error
      spy.handleStorageException(
          new InterruptedException(), 0, "download", sfSession, command, null);
      Mockito.verify(spy, Mockito.never()).renew(Mockito.anyMap());
      spy.handleStorageException(
          new InterruptedException(), 1, "download", sfSession, command, null);
    }
  }
}
