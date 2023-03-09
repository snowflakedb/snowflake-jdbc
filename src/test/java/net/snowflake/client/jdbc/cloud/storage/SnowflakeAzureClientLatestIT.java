package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.*;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.junit.Test;

public class SnowflakeAzureClientLatestIT extends BaseJDBCTest {

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testAzureClientSetupInvalidEncryptionKeySize() throws SQLException {
    try (Connection connection = getConnection("azureaccount")) {
      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      String putCommand = "put file:///dummy/path/file1.gz @~";
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
      RemoteStoreFileEncryptionMaterial content =
          new RemoteStoreFileEncryptionMaterial(
              "EXAMPLEQUERYSTAGEMASTERKEY", "EXAMPLEQUERYID", 123L);
      try {
        SnowflakeAzureClient.createSnowflakeAzureClient(sfAgent.getStageInfo(), content, sfSession);
        fail();
      } catch (SnowflakeSQLLoggedException ex) {
        assertEquals(200001, ex.getErrorCode());
      }
    }
  }
}
