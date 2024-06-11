package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.microsoft.azure.storage.blob.ListBlobItem;
import java.sql.Connection;
import java.sql.SQLException;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
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

  @Test
  public void testCloudExceptionTest() {
    Iterable<ListBlobItem> mockList = null;
    AzureObjectSummariesIterator iterator = new AzureObjectSummariesIterator(mockList);
    AzureObjectSummariesIterator spyIterator = spy(iterator);
    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> spyIterator.remove());
    assertEquals(ex.getMessage(), "remove() method not supported");
  }
}
