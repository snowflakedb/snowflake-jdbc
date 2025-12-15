package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageExtendedErrorInformation;
import java.io.IOException;
import java.util.LinkedHashMap;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.Test;

public class SnowflakeAzureClientTest {
  @Test
  public void testFormatStorageExtendedErrorInformation() {
    String expectedStr0 =
        "StorageExceptionExtendedErrorInformation: {ErrorCode= 403, ErrorMessage= Server refuses"
            + " to authorize the request, AdditionalDetails= {}}";
    String expectedStr1 =
        "StorageExceptionExtendedErrorInformation: {ErrorCode= 403, ErrorMessage= Server refuses"
            + " to authorize the request, AdditionalDetails= { key1= helloworld,key2= ,key3="
            + " fakemessage}}";
    StorageExtendedErrorInformation info = new StorageExtendedErrorInformation();
    info.setErrorCode("403");
    info.setErrorMessage("Server refuses to authorize the request");
    String formatedStr = SnowflakeAzureClient.FormatStorageExtendedErrorInformation(info);
    assertEquals(expectedStr0, formatedStr);

    LinkedHashMap<String, String[]> map = new LinkedHashMap<>();
    map.put("key1", new String[] {"hello", "world"});
    map.put("key2", new String[] {});
    map.put("key3", new String[] {"fake", "message"});
    info.setAdditionalDetails(map);
    formatedStr = SnowflakeAzureClient.FormatStorageExtendedErrorInformation(info);
    assertEquals(expectedStr1, formatedStr);
  }

  @Test
  public void testFormatStorageExtendedErrorEmptyInformation() {
    String formatedStr = SnowflakeAzureClient.FormatStorageExtendedErrorInformation(null);
    assertEquals("", formatedStr);
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

    StorageException storageException =
        new StorageException(
            "503",
            "Service Unavailable",
            503,
            new StorageExtendedErrorInformation(),
            new Exception());

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

    StorageException storageException =
        new StorageException(
            "503",
            "Service Unavailable",
            503,
            new StorageExtendedErrorInformation(),
            new Exception());

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

    StorageException storageException =
        new StorageException(
            "503", "Service Unavailable", 503, new StorageExtendedErrorInformation(), null);
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
