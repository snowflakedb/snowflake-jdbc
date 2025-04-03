package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;

import com.amazonaws.services.kms.model.UnsupportedOperationException;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
public class SnowflakeAzureClientLatestIT extends BaseJDBCTest {
  @Test
  @DontRunOnGithubActions
  public void testAzureClientSetupInvalidEncryptionKeySize() throws SQLException {
    try (Connection connection = getConnection("azureaccount")) {
      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      String putCommand = "put file:///dummy/path/file1.gz @~";
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
      RemoteStoreFileEncryptionMaterial content =
          new RemoteStoreFileEncryptionMaterial(
              "EXAMPLEQUERYSTAGEMASTERKEY", "EXAMPLEQUERYID", 123L);

      SnowflakeSQLLoggedException ex =
          assertThrows(
              SnowflakeSQLLoggedException.class,
              () ->
                  SnowflakeAzureClient.createSnowflakeAzureClient(
                      sfAgent.getStageInfo(), content, sfSession));
      assertEquals(200001, ex.getErrorCode());
    }
  }

  @Test
  public void testCloudExceptionTest() {
    Iterable<ListBlobItem> mockList = new ArrayList<>();
    AzureObjectSummariesIterator iterator = new AzureObjectSummariesIterator(mockList);
    AzureObjectSummariesIterator spyIterator = spy(iterator);
    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> spyIterator.remove());
    assertTrue(ex.getMessage().startsWith("remove() method not supported"));
  }
}
