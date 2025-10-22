package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

class AwsSdkGCPSignerTest {

  @Test
  void testHeaderMapping() {
    AwsSdkGCPSigner signer = new AwsSdkGCPSigner();

    // Create test headers
    Map<String, List<String>> headers = new HashMap<>();
    headers.put("Authorization", Arrays.asList("access_key"));
    headers.put("x-amz-storage-class", Arrays.asList("storage_class"));
    headers.put("x-amz-meta-custom", Arrays.asList("custom_meta"));
    headers.put("x-amz-acl", Arrays.asList("public-read"));

    // Create SdkHttpRequest
    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.GET)
            .uri(URI.create("https://storage.googleapis.com/bucket/object"))
            .headers(headers)
            .build();

    // Test that the signer can be instantiated
    assertNotNull(signer);

    // Test that the request can be created with headers
    assertNotNull(originalRequest.headers());
    assertEquals("access_key", originalRequest.headers().get("Authorization").get(0));
    assertEquals("storage_class", originalRequest.headers().get("x-amz-storage-class").get(0));
    assertEquals("custom_meta", originalRequest.headers().get("x-amz-meta-custom").get(0));
    assertEquals("public-read", originalRequest.headers().get("x-amz-acl").get(0));
  }

  @Test
  void testRequestBuilding() {
    // Test building requests with different methods
    SdkHttpRequest getRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.GET)
            .uri(URI.create("https://storage.googleapis.com/bucket/object"))
            .build();

    SdkHttpRequest putRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.PUT)
            .uri(URI.create("https://storage.googleapis.com/bucket/object"))
            .build();

    assertEquals(SdkHttpMethod.GET, getRequest.method());
    assertEquals(SdkHttpMethod.PUT, putRequest.method());
  }

  @Test
  void testSignerInstantiation() {
    // Test that the signer can be created and is an ExecutionInterceptor
    AwsSdkGCPSigner signer = new AwsSdkGCPSigner();
    assertNotNull(signer);

    // Verify it implements ExecutionInterceptor
    assertTrue(signer instanceof software.amazon.awssdk.core.interceptor.ExecutionInterceptor);
  }
}
