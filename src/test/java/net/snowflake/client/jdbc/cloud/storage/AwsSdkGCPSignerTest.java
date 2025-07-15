package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.Assert.assertTrue;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.http.HttpMethodName;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

class AwsSdkGCPSignerTest {

  @Test
  void testSign() {
    AWSCredentials creds = new BasicAWSCredentials("access_key", "");
    AwsSdkGCPSigner signer = new AwsSdkGCPSigner();

    DefaultRequest request = new DefaultRequest("S3");

    HashMap<String, String> headers = new HashMap<>();
    headers.put("x-amz-storage-class", "storage_class");
    headers.put("x-amz-meta-custom", "custom_meta");

    request.setHttpMethod(HttpMethodName.GET);
    request.setHeaders(headers);

    signer.sign(request, creds);

    assertTrue(request.getHeaders().get("Authorization").equals("Bearer access_key"));
    assertTrue(request.getHeaders().get("Accept-Encoding").equals("gzip,deflate"));
    assertTrue(request.getHeaders().get("x-goog-storage-class").equals("storage_class"));
    assertTrue(request.getHeaders().get("x-goog-meta-custom").equals("custom_meta"));
  }
}
