package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.HashMap;
import java.util.concurrent.CompletionException;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

class GCSAccessStrategyAwsSdkTest {

  private GCSAccessStrategyAwsSdk strategy;
  private SnowflakeGCSClient mockGcsClient;

  @BeforeEach
  void setUp() throws SnowflakeSQLException {
    // Use empty credentials (no GCS_ACCESS_TOKEN) so the constructor falls back to anonymous auth,
    // which does not probe any real endpoint.
    StageInfo stage =
        StageInfo.createStageInfo(
            "GCS", "test-bucket/path", new HashMap<>(), "US-CENTRAL1", null, null, true);
    strategy = new GCSAccessStrategyAwsSdk(stage, /* session= */ null);

    // Mock SnowflakeGCSClient to avoid real network calls and to control retry parameters.
    // getRetryBackoffMin is set to 0 so tests are not slowed down by exponential backoff.
    mockGcsClient =
        mock(SnowflakeGCSClient.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    when(mockGcsClient.getMaxRetries()).thenReturn(1);
    when(mockGcsClient.getRetryBackoffMin()).thenReturn(0);
    when(mockGcsClient.getRetryBackoffMaxExponent()).thenReturn(0);
  }

  // The key regression scenario: when uploadWithDownScopedToken catches the SdkException thrown by
  // the async S3 client (wrapped in CompletionException by .join()) it re-throws it inside a
  // SnowflakeSQLLoggedException.  The outer upload() retry loop therefore sees a three-level chain:
  //   SnowflakeSQLLoggedException → CompletionException → S3Exception(503)
  // Before the fix, handleStorageException only checked ex.getCause() (one level), which returned
  // CompletionException — not an SdkException — so the method returned false and the upload gave up
  // immediately.  After the fix the full chain is walked, S3Exception is found, and a retry occurs.
  @Test
  void testHandleStorageExceptionWrappedCompletionException503BelowMaxRetries() {
    S3Exception s3Exception =
        S3Exception.builder().message("Service Unavailable").statusCode(503).build();
    CompletionException completionException = new CompletionException(s3Exception);
    RuntimeException wrappedException =
        new RuntimeException(
            "Encountered exception during upload: " + s3Exception.getMessage(),
            completionException);

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

  // When the same doubly-wrapped S3Exception(503) arrives after the maximum number of retries has
  // been exceeded, the handler must surface it as a SnowflakeSQLException so the caller stops.
  @Test
  void testHandleStorageExceptionWrappedCompletionException503OverMaxRetries() {
    S3Exception s3Exception =
        S3Exception.builder().message("Service Unavailable").statusCode(503).build();
    CompletionException completionException = new CompletionException(s3Exception);
    RuntimeException wrappedException =
        new RuntimeException(
            "Encountered exception during upload: " + s3Exception.getMessage(),
            completionException);

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

  // A single-level wrapping (RuntimeException → S3Exception) must also be found and retried.
  // This is the simpler case that already worked before the fix; verifying it still works.
  @Test
  void testHandleStorageExceptionDirectS3Exception503BelowMaxRetries() {
    S3Exception s3Exception =
        S3Exception.builder().message("Service Unavailable").statusCode(503).build();
    RuntimeException wrappedException =
        new RuntimeException(
            "Encountered exception during upload: " + s3Exception.getMessage(), s3Exception);

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
}
