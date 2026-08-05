package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class SnowflakeGCSClientTest {

  private StageInfo createGCSStageInfo(Map<String, String> credentials) {
    return createGCSStageInfo(credentials, "US-CENTRAL1", null, null);
  }

  private StageInfo createGCSStageInfo(
      Map<String, String> credentials, String region, String endPoint, String storageAccount) {
    return StageInfo.createStageInfo(
        "GCS", "test-bucket/path", credentials, region, endPoint, storageAccount, true);
  }

  private SFSession createSession(boolean disableGcsDefaultCredentials) {
    SFSession session = new SFSession();
    session.setDisableGcsDefaultCredentials(disableGcsDefaultCredentials);
    return session;
  }

  /**
   * Core regression test for the SPCS ADC probe fix. When disableGcsDefaultCredentials is false and
   * a GCS_ACCESS_TOKEN is present, the client must still initialize successfully. Before the fix,
   * this path skipped setting explicit credentials on StorageOptions.Builder, causing the GCS SDK
   * to probe metadata.google.internal via ADC — which fails in SPCS and on any non-GCP host.
   */
  @Test
  void testClientCreationSucceedsWithDisabledDefaultCredentialsFalse() {
    Map<String, String> credentials = new HashMap<>();
    credentials.put("GCS_ACCESS_TOKEN", "test-token");
    StageInfo stage = createGCSStageInfo(credentials);

    SFSession session = createSession(false);

    assertDoesNotThrow(
        () -> SnowflakeGCSClient.createSnowflakeGCSClient(stage, null, session),
        "GCS client should initialize without ADC probe even when"
            + " disableGcsDefaultCredentials is false");
  }

  /**
   * Verifies that setupGCSClient chains the original exception as the cause of the
   * IllegalArgumentException instead of silently swallowing it. Before the fix, the catch block
   * threw new IllegalArgumentException("invalid_gcs_credentials") with no cause, making root-cause
   * diagnosis impossible.
   */
  @Test
  void testSetupGCSClientChainsExceptionCause() {
    Map<String, String> credentials = new HashMap<>();
    credentials.put("GCS_ACCESS_TOKEN", "test-token");
    StageInfo stage = createGCSStageInfo(credentials);
    SFSession session = createSession(true);

    RemoteStoreFileEncryptionMaterial encMat =
        new RemoteStoreFileEncryptionMaterial("not-valid-base64!@#$", "queryId", 123L);

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SnowflakeGCSClient.createSnowflakeGCSClient(stage, encMat, session));
    assertEquals("invalid_gcs_credentials", ex.getMessage());
    assertNotNull(ex.getCause(), "Original exception should be chained as cause");
  }

  /**
   * Verifies that SnowflakeSQLException from the encryption key size validation propagates directly
   * instead of being wrapped in IllegalArgumentException. Before the fix, the broad catch
   * (Exception ex) caught SnowflakeSQLException and re-threw it as IllegalArgumentException, losing
   * the specific error type and message.
   */
  @Test
  void testSnowflakeSQLExceptionPropagatesDirectly() {
    Map<String, String> credentials = new HashMap<>();
    credentials.put("GCS_ACCESS_TOKEN", "test-token");
    StageInfo stage = createGCSStageInfo(credentials);
    SFSession session = createSession(true);

    // 10-byte key (80 bits) — not a valid key size (must be 128, 192, or 256)
    byte[] invalidSizeKey = new byte[10];
    String encodedKey = Base64.getEncoder().encodeToString(invalidSizeKey);
    RemoteStoreFileEncryptionMaterial encMat =
        new RemoteStoreFileEncryptionMaterial(encodedKey, "queryId", 123L);

    assertThrows(
        SnowflakeSQLException.class,
        () -> SnowflakeGCSClient.createSnowflakeGCSClient(stage, encMat, session),
        "SnowflakeSQLException should propagate directly, not wrapped in IllegalArgumentException");
  }

  /**
   * Verifies that GCSAccessStrategyAwsSdk prepends https:// to custom endpoints that lack a scheme.
   * Before the fix, a bare hostname like "storage.me-central2.rep.googleapis.com" was passed to the
   * AWS SDK's URI parser as-is, which rejected it with NullPointerException because the URI scheme
   * was null.
   */
  @Test
  void testAwsSdkStrategyPrependsSchemeToBarHostnameEndpoint() {
    Map<String, String> credentials = new HashMap<>();
    credentials.put("GCS_ACCESS_TOKEN", "test-token");

    StageInfo stage =
        createGCSStageInfo(
            credentials, "ME-CENTRAL2", "storage.me-central2.rep.googleapis.com", null);
    stage.setUseVirtualUrl(true);

    SFSession session = createSession(true);

    assertDoesNotThrow(
        () -> SnowflakeGCSClient.createSnowflakeGCSClient(stage, null, session),
        "Bare hostname endpoint should get https:// prepended automatically");
  }

  /**
   * Verifies that GCSAccessStrategyAwsSdk does not double-prefix endpoints that already have a
   * scheme.
   */
  @Test
  void testAwsSdkStrategyPreservesEndpointWithScheme() {
    Map<String, String> credentials = new HashMap<>();
    credentials.put("GCS_ACCESS_TOKEN", "test-token");

    StageInfo stage =
        createGCSStageInfo(
            credentials, "ME-CENTRAL2", "https://storage.me-central2.rep.googleapis.com", null);
    stage.setUseVirtualUrl(true);

    SFSession session = createSession(true);

    assertDoesNotThrow(
        () -> SnowflakeGCSClient.createSnowflakeGCSClient(stage, null, session),
        "Endpoint with https:// prefix should be passed through as-is");
  }

  /**
   * SNOW-3888537: guard for the GCS S3-interop checksum fix. On this path the upload body is a
   * streaming AsyncRequestBody, so AWS SDK v2 can only deliver a flexible checksum as an
   * aws-chunked trailer, which GCS stores verbatim and corrupts the object. The fix has two halves,
   * both load-bearing: {@code S3ObjectMetadata.setRequestIntegrityChecksum(false)} clears the
   * explicit CRC32 on the request, and the client is built with {@code
   * RequestChecksumCalculation.WHEN_REQUIRED} so the SDK default (WHEN_SUPPORTED since 2.30.0)
   * doesn't re-add one. This test mirrors those two settings, builds the request through the real
   * {@link S3ObjectMetadata} path, and asserts via a capturing {@link ExecutionInterceptor} (the
   * same interceptor mechanism the strategy itself uses) that the emitted PUT carries no
   * flexible-checksum header/trailer and is not aws-chunked framed -- asserting behavior, not a
   * private config field. The authoritative regression guard is the live e2e {@code
   * SnowflakeDriverIT.testPutCopyIntoWith256BitEncryptionOnAllAccounts} (gcpaccount_awssdk); this
   * is the fast, offline complement.
   */
  @Test
  void testAwsSdkStrategyEmitsNoChecksumTrailerOnGcsPut() throws Exception {
    CapturingHttpRequestInterceptor interceptor = new CapturingHttpRequestInterceptor();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    byte[] payload = "PAR1-streamed-body".getBytes(StandardCharsets.UTF_8);

    try (S3AsyncClient client =
        S3AsyncClient.builder()
            .region(Region.US_WEST_2)
            .forcePathStyle(false)
            .endpointOverride(URI.create("https://storage.googleapis.com"))
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
            .httpClient(new NoNetworkAsyncHttpClient())
            .overrideConfiguration(o -> o.addExecutionInterceptor(interceptor))
            .build()) {

      S3ObjectMetadata metadata = new S3ObjectMetadata();
      metadata.setContentLength((long) payload.length);
      metadata.setRequestIntegrityChecksum(false);
      PutObjectRequest request =
          metadata.getS3PutObjectRequest().toBuilder().bucket("bucket").key("key").build();

      try {
        client
            .putObject(
                request,
                AsyncRequestBody.fromInputStream(
                    new ByteArrayInputStream(payload), (long) payload.length, executor))
            .join();
      } catch (RuntimeException ignore) {
        // The stub http client fails the transmission on purpose. We only need the request the SDK
        // emitted, which the interceptor captured (before transmission) with its final headers.
      }
    } finally {
      executor.shutdownNow();
    }

    SdkHttpRequest emitted = interceptor.capturedRequest();
    assertNotNull(emitted, "no request was emitted to the HTTP client");
    for (String header : emitted.headers().keySet()) {
      String lower = header.toLowerCase(Locale.ROOT);
      assertFalse(
          lower.startsWith("x-amz-checksum-") || lower.equals("x-amz-trailer"),
          "GCS S3-interop PUT must have no flexible-checksum header/trailer (SNOW-3888537): "
              + header);
    }
    assertFalse(
        emitted.firstMatchingHeader("Content-Encoding").orElse("").contains("aws-chunked"),
        "GCS S3-interop PUT must not be aws-chunked framed (SNOW-3888537)");
  }

  /** Captures the final HTTP request the SDK is about to transmit, for behavioral assertions. */
  private static final class CapturingHttpRequestInterceptor implements ExecutionInterceptor {
    private volatile SdkHttpRequest httpRequest;

    @Override
    public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes attrs) {
      this.httpRequest = context.httpRequest();
    }

    SdkHttpRequest capturedRequest() {
      return httpRequest;
    }
  }

  /**
   * Minimal async HTTP client that makes no network call. By the time {@code execute} runs, the SDK
   * has already fired {@code beforeTransmission} and finalized the request headers (signing +
   * flexible-checksum stages), so the interceptor has captured everything we assert on. We simply
   * fail the request fast -- without subscribing to the body or emitting a response -- so the stub
   * needs no reactive-streams plumbing. A plain (non-IOException) failure is not retried, so this
   * runs a single attempt.
   */
  private static final class NoNetworkAsyncHttpClient implements SdkAsyncHttpClient {
    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      Throwable error = new UnsupportedOperationException("no network in unit test");
      // Signal failure through the response handler so the operation future completes (otherwise
      // the SDK waits for a response that never arrives), then fail the transmission future too.
      request.responseHandler().onError(error);
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(error);
      return future;
    }

    @Override
    public String clientName() {
      return "no-network";
    }

    @Override
    public void close() {}
  }
}
