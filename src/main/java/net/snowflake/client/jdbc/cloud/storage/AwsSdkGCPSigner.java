package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.http.HttpMethodName;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class AwsSdkGCPSigner extends AWS4Signer {
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
  public void sign(SignableRequest<?> request, AWSCredentials credentials) {
    if (credentials.getAWSAccessKeyId() != null && !"".equals(credentials.getAWSAccessKeyId())) {
      request.addHeader("Authorization", "Bearer " + credentials.getAWSAccessKeyId());
    }

    if (request.getHttpMethod() == HttpMethodName.GET) {
      request.addHeader("Accept-Encoding", "gzip,deflate");
    }

    Map<String, String> headerCopy =
        request.getHeaders().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    for (Map.Entry<String, String> entry : headerCopy.entrySet()) {
      String entryKey = entry.getKey().toLowerCase();
      if (headerMap.containsKey(entryKey)) {
        request.addHeader(headerMap.get(entryKey), entry.getValue());
      } else if (entryKey.startsWith("x-amz-meta-")) {
        request.addHeader(entryKey.replace("x-amz-meta-", "x-goog-meta-"), entry.getValue());
      }
    }
  }
}
