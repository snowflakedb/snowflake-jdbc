package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;

class AwsSdkGCPSignerTest {
  @Test
  void testSign() {
    AwsSdkGCPSigner signer = new AwsSdkGCPSigner("test_token");

    SdkHttpFullRequest.Builder request =
        SdkHttpFullRequest.builder().protocol("https").host("localhost").method(SdkHttpMethod.GET);

    request.putHeader("x-amz-storage-class", "storage_class");
    request.putHeader("x-amz-meta-custom", "custom_meta");

    SdkHttpFullRequest signed = signer.sign(request.build(), null);
    Map<String, List<String>> changedHeaders = signed.headers();

    assertEquals("Bearer test_token", changedHeaders.get("Authorization").get(0));
    assertEquals("gzip,deflate", changedHeaders.get("Accept-Encoding").get(0));
    assertEquals("storage_class", changedHeaders.get("x-goog-storage-class").get(0));
    assertEquals("custom_meta", changedHeaders.get("x-goog-meta-custom").get(0));
  }
}
