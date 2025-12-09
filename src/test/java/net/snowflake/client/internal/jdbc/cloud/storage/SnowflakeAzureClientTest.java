package net.snowflake.client.internal.jdbc.cloud.storage;

import static com.azure.storage.common.implementation.Constants.HeaderConstants.ERROR_CODE_HEADER_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import java.io.IOException;
import net.snowflake.client.api.exception.SnowflakeSQLException;
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

  // We should not throw a SnowflakeSQLException when a 503 occurs if we still haven't reached the
  // maximum number of retries
  @Test
  public void testHandleStorageExceptionRetry503() throws Exception {
    SnowflakeAzureClient client =
        mock(SnowflakeAzureClient.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    when(client.getMaxRetries()).thenReturn(1);
    when(client.getRetryBackoffMin()).thenReturn(0);
    when(client.getRetryBackoffMaxExponent()).thenReturn(0);

    HttpResponse response = mock(HttpResponse.class);
    when(response.getHeaders()).thenReturn(new HttpHeaders().add(ERROR_CODE_HEADER_NAME, "503"));
    BlobStorageException storageException =
        new BlobStorageException("Service Unavailable", response, new Exception());

    assertDoesNotThrow(
        () ->
            client.handleStorageException(
                storageException, /* retryCount */
                1,
                "upload", /* session */
                null, /* command */
                "PUT",
                null));
  }

  // We should throw a SnowflakeSQLException when we receive a 503 after reaching the maximum number
  // of retries.
  @Test
  public void testHandleStorageException503WhenOverMaxRetries() throws Exception {
    SnowflakeAzureClient client =
        mock(SnowflakeAzureClient.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    when(client.getMaxRetries()).thenReturn(1);

    HttpResponse response = mock(HttpResponse.class);
    when(response.getHeaders()).thenReturn(new HttpHeaders().add(ERROR_CODE_HEADER_NAME, "503"));
    BlobStorageException storageException =
        new BlobStorageException("Service Unavailable", response, new Exception());

    assertThrows(
        SnowflakeSQLException.class,
        () ->
            client.handleStorageException(
                storageException, /* retryCount */
                2,
                "upload", /* session */
                null, /* command */
                "PUT",
                null));
  }

  // We should retry IOExceptions caused by StorageExceptions
  @Test
  public void testHandleStorageExceptionIOExceptionCausedBy503() throws Exception {
    SnowflakeAzureClient client =
        mock(SnowflakeAzureClient.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    when(client.getMaxRetries()).thenReturn(1);
    when(client.getRetryBackoffMin()).thenReturn(0);
    when(client.getRetryBackoffMaxExponent()).thenReturn(0);

    HttpResponse response = mock(HttpResponse.class);
    when(response.getHeaders()).thenReturn(new HttpHeaders().add(ERROR_CODE_HEADER_NAME, "503"));
    BlobStorageException storageException =
        new BlobStorageException("Service Unavailable", response, new Exception());
    IOException ioException = new IOException("transport error", storageException);

    assertDoesNotThrow(
        () ->
            client.handleStorageException(
                ioException, /* retryCount */
                1,
                "upload", /* session */
                null, /* command */
                "PUT",
                null));
  }
}
