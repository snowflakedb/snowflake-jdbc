package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;

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
   * SNOW-3888537: the GCS S3-interop client must be built with
   * RequestChecksumCalculation.WHEN_REQUIRED. This is load-bearing, not optional: the AWS SDK v2
   * default is WHEN_SUPPORTED, which would auto-add a CRC32 to the streaming PUT (and to the
   * multipart part requests the TransferManager may create) even though the PutObjectRequest itself
   * no longer sets an explicit checksum. GCS stores the resulting aws-chunked trailer verbatim and
   * corrupts the object, so removing this line silently reintroduces the corruption. This guards
   * the half that S3ObjectMetadataTest cannot see (the request-level checksum is cleared there).
   */
  @Test
  void testAwsSdkStrategyBuildsClientWithChecksumWhenRequired() throws Exception {
    Map<String, String> credentials = new HashMap<>();
    credentials.put("GCS_ACCESS_TOKEN", "test-token");
    StageInfo stage = createGCSStageInfo(credentials);
    stage.setUseVirtualUrl(true);
    SFSession session = createSession(true);

    GCSAccessStrategyAwsSdk strategy = new GCSAccessStrategyAwsSdk(stage, session);
    // The constructor allocates real S3AsyncClient resources (Netty event loop, executor) even
    // though this test makes no network call, so always release them.
    try {
      // Intentionally white-box: there is no public API to read a built client's checksum
      // configuration, so we reflect into the S3AsyncClient's SdkClientConfiguration. This is
      // coupled to AWS SDK internals (field is looked up by type to survive a rename) and may need
      // updating on a major SDK upgrade or if the client is ever wrapped by a decorator.
      Object s3Client = readField(strategy, "amazonClient");
      SdkClientConfiguration clientConfiguration =
          (SdkClientConfiguration) readFieldOfType(s3Client, SdkClientConfiguration.class);
      RequestChecksumCalculation checksumCalculation =
          clientConfiguration.option(SdkClientOption.REQUEST_CHECKSUM_CALCULATION);

      assertEquals(
          RequestChecksumCalculation.WHEN_REQUIRED,
          checksumCalculation,
          "GCS S3-interop client must be built with WHEN_REQUIRED so the SDK never auto-adds a "
              + "checksum that GCS would store verbatim (SNOW-3888537)");
    } finally {
      strategy.shutdown();
    }
  }

  private static Object readField(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  private static Object readFieldOfType(Object target, Class<?> type) throws Exception {
    for (Class<?> c = target.getClass(); c != null; c = c.getSuperclass()) {
      for (Field field : c.getDeclaredFields()) {
        if (type.isAssignableFrom(field.getType())) {
          field.setAccessible(true);
          return field.get(target);
        }
      }
    }
    throw new NoSuchFieldException(
        "no field of type " + type.getName() + " on " + target.getClass().getName());
  }
}
