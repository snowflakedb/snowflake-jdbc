package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.storage.StorageException;
import java.util.HashMap;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GCSDefaultAccessStrategyTest {

  private GCSDefaultAccessStrategy strategy;
  private SnowflakeGCSClient mockGcsClient;

  @BeforeEach
  void setUp() throws SnowflakeSQLException {
    // Use empty credentials (no GCS_ACCESS_TOKEN) so the constructor falls back to anonymous auth,
    // which does not probe any real GCS endpoint.
    StageInfo stage =
        StageInfo.createStageInfo(
            "GCS", "test-bucket/path", new HashMap<>(), "US-CENTRAL1", null, null, true);
    strategy = new GCSDefaultAccessStrategy(stage, /* session= */ null);

    // Mock SnowflakeGCSClient to avoid real network calls and to control retry parameters.
    // getRetryBackoffMin is set to 0 so tests are not slowed down by exponential backoff.
    mockGcsClient =
        mock(SnowflakeGCSClient.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    when(mockGcsClient.getMaxRetries()).thenReturn(1);
    when(mockGcsClient.getRetryBackoffMin()).thenReturn(0);
    when(mockGcsClient.getRetryBackoffMaxExponent()).thenReturn(0);
  }

  // A direct StorageException(503) below the max retry threshold must not be thrown — the handler
  // should perform a backoff and signal to the caller that a retry should proceed.
  @Test
  void testHandleStorageExceptionDirect503BelowMaxRetries() {
    StorageException storageException = new StorageException(503, "Service Unavailable");

    assertDoesNotThrow(
        () ->
            strategy.handleStorageException(
                storageException,
                /* retryCount= */ 1,
                "upload",
                /* session= */ null,
                /* command= */ null,
                /* queryId= */ null,
                mockGcsClient));
  }

  // A StorageException(503) wrapped inside another exception must also be detected via cause-chain
  // walking and treated as retryable when below the max retry threshold.  This is the key
  // regression scenario: uploadWithDownScopedToken wraps the original StorageException inside a
  // SnowflakeSQLException before re-throwing it to the outer upload() retry loop.
  @Test
  void testHandleStorageExceptionWrapped503BelowMaxRetries() {
    StorageException storageException = new StorageException(503, "Service Unavailable");
    RuntimeException wrappedException =
        new RuntimeException(
            "Encountered exception during upload: " + storageException.getMessage(),
            storageException);

    assertDoesNotThrow(
        () ->
            strategy.handleStorageException(
                wrappedException,
                /* retryCount= */ 1,
                "upload",
                /* session= */ null,
                /* command= */ null,
                /* queryId= */ null,
                mockGcsClient));
  }

  // When a wrapped StorageException(503) arrives after the maximum number of retries has been
  // exceeded the handler must surface it as a SnowflakeSQLException so the caller stops retrying.
  @Test
  void testHandleStorageExceptionWrapped503OverMaxRetries() {
    StorageException storageException = new StorageException(503, "Service Unavailable");
    RuntimeException wrappedException =
        new RuntimeException(
            "Encountered exception during upload: " + storageException.getMessage(),
            storageException);

    assertThrows(
        SnowflakeSQLException.class,
        () ->
            strategy.handleStorageException(
                wrappedException,
                /* retryCount= */ 2,
                "upload",
                /* session= */ null,
                /* command= */ null,
                /* queryId= */ null,
                mockGcsClient));
  }
}
