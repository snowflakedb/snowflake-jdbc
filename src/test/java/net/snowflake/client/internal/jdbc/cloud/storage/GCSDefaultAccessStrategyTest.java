package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpMediaType;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.http.HttpTransportFactory;
import com.google.cloud.storage.StorageException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testUploadDoesNotGzipHttpRequest(boolean hasAccessToken) throws Exception {
    byte[] content = new byte[4096];
    new Random(0).nextBytes(content);
    ByteArrayOutputStream requestBody = new ByteArrayOutputStream();
    MockLowLevelHttpRequest request =
        new MockLowLevelHttpRequest() {
          @Override
          public LowLevelHttpResponse execute() throws IOException {
            getStreamingContent().writeTo(requestBody);
            return super.execute();
          }
        };
    request.setResponse(
        new MockLowLevelHttpResponse()
            .setContentType("application/json")
            .setContent("{\"bucket\":\"test-bucket\",\"name\":\"file.gz\"}"));
    MockHttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();
    HttpTransportFactory transportFactory = () -> transport;
    Map<String, String> credentials = new HashMap<>();
    if (hasAccessToken) {
      credentials.put("GCS_ACCESS_TOKEN", "test-token");
    }
    StageInfo stage =
        StageInfo.createStageInfo(
            "GCS", "test-bucket/path", credentials, "US-CENTRAL1", null, null, true);

    try (MockedStatic<CloudStorageProxyFactory> proxyFactory =
        mockStatic(CloudStorageProxyFactory.class)) {
      proxyFactory
          .when(() -> CloudStorageProxyFactory.createSessionlessHttpTransportForGCS(null))
          .thenReturn(transportFactory);
      GCSDefaultAccessStrategy uploadStrategy = new GCSDefaultAccessStrategy(stage, null);
      uploadStrategy.uploadWithDownScopedToken(
          1,
          "test-bucket",
          "file.gz",
          "gzip",
          Collections.singletonMap("encryptiondata", "test-encryption-metadata"),
          content.length,
          new ByteArrayInputStream(content),
          "query-id");
    }

    assertNull(
        request.getContentEncoding(), "Already encrypted/compressed media must not be gzipped");
    String multipart = new String(requestBody.toByteArray(), StandardCharsets.ISO_8859_1);
    String boundary = new HttpMediaType(request.getContentType()).getParameter("boundary");
    int metadataStart = multipart.indexOf("\r\n\r\n") + 4;
    int metadataEnd = multipart.indexOf("\r\n--" + boundary, metadataStart);
    assertTrue(metadataStart >= 4 && metadataEnd > metadataStart);
    JsonNode metadata =
        new ObjectMapper().readTree(multipart.substring(metadataStart, metadataEnd));
    assertEquals("test-bucket", metadata.get("bucket").asText());
    assertEquals("file.gz", metadata.get("name").asText());
    assertEquals("gzip", metadata.get("contentEncoding").asText());
    assertEquals(
        "test-encryption-metadata", metadata.get("metadata").get("encryptiondata").asText());
    int contentStart = multipart.indexOf("\r\n\r\n", metadataEnd) + 4;
    int contentEnd = multipart.lastIndexOf("\r\n--" + boundary + "--");
    assertTrue(contentStart > metadataEnd && contentEnd > contentStart);
    assertArrayEquals(
        content, Arrays.copyOfRange(requestBody.toByteArray(), contentStart, contentEnd));
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
