package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import org.junit.jupiter.api.Test;

public class SnowflakeAzureClientTest {
  @Test
  public void testFormatStorageExtendedErrorInformation() {
    String expectedStr0 =
        "StorageExceptionExtendedErrorInformation: {ErrorCode=AuthorizationFailure, ErrorMessage=Server refuses"
            + " to authorize the request}";
    BlobStorageException info = mock(BlobStorageException.class);
    when(info.getErrorCode()).thenReturn(BlobErrorCode.AUTHORIZATION_FAILURE);
    when(info.getServiceMessage()).thenReturn("Server refuses to authorize the request");
    String formatedStr = SnowflakeAzureClient.formatStorageExtendedErrorInformation(info);
    assertEquals(expectedStr0, formatedStr);
  }
}
