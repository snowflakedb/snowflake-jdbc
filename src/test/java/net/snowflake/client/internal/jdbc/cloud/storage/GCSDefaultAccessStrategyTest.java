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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.HttpClientSettingsKey;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.core.SFSessionProperty;
import net.snowflake.client.internal.jdbc.MatDesc;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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

  private static Stream<Arguments> uploadCompressionCases() {
    Object[][] cases = {
      // Encoding, encryption metadata, session gzipDisabled (null means no session), HTTP gzip.
      {null, false, null, true},
      {null, true, null, false},
      {"gzip", false, null, false},
      {null, false, false, true},
      {null, false, true, false},
      {null, true, false, false},
      {null, true, true, false},
      {"gzip", false, false, false},
      {"gzip", false, true, false},
      {"zstd", true, false, false},
      {"GZIP", false, false, false},
      {"x-gzip", false, false, false},
      {"zlib", false, false, false},
      {"deflate", false, false, false},
      {"raw_deflate", false, false, false},
      {"bzip2", false, false, false},
      {"zstd", false, false, false},
      {"brotli", false, false, false},
      {"", false, false, true},
      {"identity", false, false, true},
      {"none", false, false, true},
      {"unknown", false, false, true},
      {"unknown", false, true, false},
      {"parquet", false, false, true},
      {"orc", false, false, true},
    };
    return Stream.of(false, true)
        .flatMap(
            hasAccessToken ->
                Arrays.stream(cases)
                    .map(
                        input ->
                            Arguments.of(hasAccessToken, input[0], input[1], input[2], input[3])));
  }

  @ParameterizedTest(
      name = "token={0}, encoding={1}, encrypted={2}, gzipDisabled={3}, httpGzip={4}")
  @MethodSource("uploadCompressionCases")
  void testUploadHttpCompressionPolicy(
      boolean hasAccessToken,
      String contentEncoding,
      boolean isEncrypted,
      Boolean gzipDisabled,
      boolean expectHttpGzip)
      throws Throwable {
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
    SFSession session = gzipDisabled == null ? null : new SFSession();
    if (session != null) {
      session.addProperty(SFSessionProperty.GZIP_DISABLED.getPropertyKey(), gzipDisabled);
    }
    HttpClientSettingsKey settings = session == null ? null : session.getHttpClientKey();
    CommonObjectMetadata uploadMetadata = new CommonObjectMetadata();
    uploadMetadata.addUserMetadata("test-key", "test-value");
    if (isEncrypted) {
      mockGcsClient.addEncryptionMetadata(
          uploadMetadata,
          new MatDesc(123, "query-id", 128),
          new byte[16],
          new byte[16],
          content.length);
    }

    try (MockedStatic<CloudStorageProxyFactory> proxyFactory =
        mockStatic(CloudStorageProxyFactory.class)) {
      if (session == null) {
        proxyFactory
            .when(() -> CloudStorageProxyFactory.createSessionlessHttpTransportForGCS(null))
            .thenReturn(transportFactory);
      } else {
        proxyFactory
            .when(() -> CloudStorageProxyFactory.createHttpTransportForGCS(settings))
            .thenReturn(transportFactory);
      }
      GCSDefaultAccessStrategy uploadStrategy = new GCSDefaultAccessStrategy(stage, session);
      uploadStrategy.uploadWithDownScopedToken(
          1,
          "test-bucket",
          "file.gz",
          contentEncoding,
          uploadMetadata.getUserMetadata(),
          content.length,
          new ByteArrayInputStream(content),
          "query-id");
    }

    byte[] multipartBytes = requestBody.toByteArray();
    if (expectHttpGzip) {
      assertEquals("gzip", request.getContentEncoding());
      try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(multipartBytes))) {
        multipartBytes = IOUtils.toByteArray(gzip);
      }
    } else {
      assertNull(request.getContentEncoding());
    }
    String multipart = new String(multipartBytes, StandardCharsets.ISO_8859_1);
    String boundary = new HttpMediaType(request.getContentType()).getParameter("boundary");
    int metadataStart = multipart.indexOf("\r\n\r\n") + 4;
    int metadataEnd = multipart.indexOf("\r\n--" + boundary, metadataStart);
    assertTrue(metadataStart >= 4 && metadataEnd > metadataStart);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode metadata = mapper.readTree(multipart.substring(metadataStart, metadataEnd));
    assertEquals("test-bucket", metadata.get("bucket").asText());
    assertEquals("file.gz", metadata.get("name").asText());
    assertEquals(contentEncoding, metadata.path("contentEncoding").textValue());
    assertEquals(mapper.valueToTree(uploadMetadata.getUserMetadata()), metadata.get("metadata"));
    int contentStart = multipart.indexOf("\r\n\r\n", metadataEnd) + 4;
    int contentEnd = multipart.lastIndexOf("\r\n--" + boundary + "--");
    assertTrue(contentStart > metadataEnd && contentEnd > contentStart);
    assertArrayEquals(content, Arrays.copyOfRange(multipartBytes, contentStart, contentEnd));
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
