package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

class GCSAccessStrategyAwsSdkTest {

  /**
   * GCS returns the user metadata written by {@link AwsSdkGCPSigner} as {@code x-goog-meta-*}
   * response headers and leaves {@link HeadObjectResponse#metadata()} empty, since the S3 model
   * fills that map from {@code x-amz-meta-*} only. Losing the headers makes an encrypted GET write
   * ciphertext to the destination and still report success.
   */
  @Test
  void testUserMetadataIsReadFromGoogleResponseHeaders() {
    HeadObjectResponse response =
        headObjectResponse(
            SdkHttpResponse.builder()
                .statusCode(200)
                .putHeader("x-goog-meta-encryptiondata", "{\"ContentEncryptionIV\":\"iv\"}")
                .putHeader("X-Goog-Meta-Sfc-Digest", "digest")
                .putHeader("Content-Length", "42")
                .build());

    Map<String, String> metadata = GCSAccessStrategyAwsSdk.userMetadata(response);

    assertEquals(2, metadata.size());
    assertEquals("{\"ContentEncryptionIV\":\"iv\"}", metadata.get("encryptiondata"));
    assertEquals("digest", metadata.get("Sfc-Digest"));
  }

  @Test
  void testUserMetadataIsEmptyWhenResponseCarriesNoGoogleMetadataHeaders() {
    HeadObjectResponse response =
        headObjectResponse(
            SdkHttpResponse.builder().statusCode(200).putHeader("Content-Length", "42").build());

    assertEquals(Collections.emptyMap(), GCSAccessStrategyAwsSdk.userMetadata(response));
  }

  /** Mirrors a GCS reply: user metadata only in the raw headers, never in the S3 metadata map. */
  private static HeadObjectResponse headObjectResponse(SdkHttpResponse httpResponse) {
    return (HeadObjectResponse)
        HeadObjectResponse.builder()
            .metadata(Collections.emptyMap())
            .sdkHttpResponse(httpResponse)
            .build();
  }
}
