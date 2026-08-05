package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
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
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
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
   * SNOW-3888537 request-half guard (the load-bearing fix). Drives the REAL production client
   * ({@link GCSAccessStrategyAwsSdk}, GCP signer on) with only a no-network HTTP client and a
   * capturing interceptor swapped in, and asserts the emitted GCS PUT carries no flexible-checksum
   * header/trailer and is not aws-chunked framed. Positive control: re-enabling the explicit
   * checksum turns it red even with the signer present.
   */
  @Test
  void testAwsSdkStrategyEmitsNoChecksumTrailerOnGcsPut() throws Exception {
    SdkHttpRequest emitted = emitGcsPutAndCapture(/* installGcpSigner= */ true);
    assertNoFlexibleChecksum(emitted, "GCS S3-interop PUT (production client)");
  }

  /**
   * SNOW-3888537 client-half guard (future-proofing). WHEN_REQUIRED is inert on the prod path (the
   * GCP signer masks the SDK's WHEN_SUPPORTED auto-checksum), so this test drops the signer ({@code
   * installGcpSigner=false}) to make the client-level calc observable and asserts the PUT still has
   * no checksum trailer. Honest scope: guards the config line under a non-prod signer. Positive
   * control: flipping the client to WHEN_SUPPORTED turns it red.
   */
  @Test
  void testClientLevelChecksumCalcIsWhenRequired() throws Exception {
    SdkHttpRequest emitted = emitGcsPutAndCapture(/* installGcpSigner= */ false);
    assertNoFlexibleChecksum(emitted, "GCS S3-interop client (WHEN_REQUIRED, signer suppressed)");
  }

  /**
   * Builds the real production GCS client via the test seam (no-network HTTP client + capturing
   * interceptor), issues a streaming PUT built through the real {@link S3ObjectMetadata} opt-out
   * path, and returns the request the SDK actually emitted (captured pre-transmission).
   */
  private SdkHttpRequest emitGcsPutAndCapture(boolean installGcpSigner) throws Exception {
    CapturingHttpRequestInterceptor interceptor = new CapturingHttpRequestInterceptor();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    byte[] payload = "PAR1-streamed-body".getBytes(StandardCharsets.UTF_8);

    Map<String, String> credentials = new HashMap<>();
    credentials.put("GCS_ACCESS_TOKEN", "test-token");
    StageInfo stage = createGCSStageInfo(credentials);
    stage.setUseVirtualUrl(true);
    SFSession session = createSession(true);

    GCSAccessStrategyAwsSdk strategy =
        new GCSAccessStrategyAwsSdk(
            stage, session, new NoNetworkAsyncHttpClient(), interceptor, installGcpSigner);
    try {
      S3AsyncClient client = strategy.clientForTest();

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
        // Stub fails transmission on purpose; the interceptor already captured the request.
      }
    } finally {
      executor.shutdownNow();
      strategy.shutdown();
    }

    SdkHttpRequest emitted = interceptor.capturedRequest();
    assertNotNull(emitted, "no request was emitted to the HTTP client");
    return emitted;
  }

  private static void assertNoFlexibleChecksum(SdkHttpRequest emitted, String context) {
    for (String header : emitted.headers().keySet()) {
      String lower = header.toLowerCase(Locale.ROOT);
      assertFalse(
          lower.startsWith("x-amz-checksum-") || lower.equals("x-amz-trailer"),
          context + " must carry no flexible-checksum header/trailer (SNOW-3888537): " + header);
    }
    assertFalse(
        emitted.firstMatchingHeader("Content-Encoding").orElse("").contains("aws-chunked"),
        context + " must not be aws-chunked framed (SNOW-3888537)");
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
   * Async HTTP client that makes no network call. By the time {@code execute} runs the SDK has
   * already fired {@code beforeTransmission} with final headers, so the interceptor has what we
   * assert on. It fails fast without subscribing to the body (no reactive-streams plumbing needed);
   * a non-IOException failure is not retried, so this is a single attempt.
   */
  private static final class NoNetworkAsyncHttpClient implements SdkAsyncHttpClient {
    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      Throwable error = new UnsupportedOperationException("no network in unit test");
      // Complete via the response handler (else the SDK waits forever), then fail the future.
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
