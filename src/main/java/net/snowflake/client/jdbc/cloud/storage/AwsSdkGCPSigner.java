package net.snowflake.client.jdbc.cloud.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;

@SnowflakeJdbcInternalApi
public class AwsSdkGCPSigner implements Signer {
  private final String bearerToken;

  public AwsSdkGCPSigner(String bearerToken) {
    this.bearerToken = bearerToken;
  }

  private static final Map<String, String> headerMap =
      new HashMap<String, String>() {
        {
          put("x-amz-storage-class", "x-goog-storage-class");
          put("x-amz-acl", "x-goog-acl");
          put("x-amz-date", "x-goog-date");
          put("x-amz-copy-source", "x-goog-copy-source");
          put("x-amz-metadata-directive", "x-goog-metadata-directive");
          put("x-amz-copy-source-if-match", "x-goog-copy-source-if-match");
          put("x-amz-copy-source-if-none-match", "x-goog-copy-source-if-none-match");
          put("x-amz-copy-source-if-unmodified-since", "x-goog-copy-source-if-unmodified-since");
          put("x-amz-copy-source-if-modified-since", "x-goog-copy-source-if-modified-since");
        }
      };

  @Override
  public SdkHttpFullRequest sign(
      SdkHttpFullRequest request, ExecutionAttributes executionAttributes) {
    SdkHttpFullRequest.Builder requestBuilder = request.toBuilder();

    // Remove any existing Authorization header (from AWS signing)
    requestBuilder.removeHeader("Authorization");

    // Add the Bearer token for GCP authentication
    if (bearerToken != null && !bearerToken.isEmpty()) {
      requestBuilder.putHeader("Authorization", "Bearer " + bearerToken);
    }

    if (request.method() == SdkHttpMethod.GET) {
      requestBuilder.putHeader("Accept-Encoding", "gzip,deflate");
    }

    // Create a copy of headers for iteration to avoid concurrent modification
    Map<String, List<String>> headersCopy = new HashMap<>(request.headers());

    for (Map.Entry<String, List<String>> entry : headersCopy.entrySet()) {
      String entryKey = entry.getKey().toLowerCase();
      if (headerMap.containsKey(entryKey)) {
        // Add the mapped Google Cloud header
        for (String value : entry.getValue()) {
          requestBuilder.putHeader(headerMap.get(entryKey), value);
        }
      } else if (entryKey.startsWith("x-amz-meta-")) {
        // Transform x-amz-meta- headers to x-goog-meta-
        String googleMetaHeader = entryKey.replace("x-amz-meta-", "x-goog-meta-");
        for (String value : entry.getValue()) {
          requestBuilder.putHeader(googleMetaHeader, value);
        }
      }
    }

    return requestBuilder.build();
  }
}
