package net.snowflake.client.internal.core;

import java.net.URI;
import java.security.cert.X509Certificate;
import java.time.Duration;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.TlsTrustManagersProvider;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Drives an {@link S3AsyncClient} built the way {@code SnowflakeS3Client} builds it -- AWS SDK v2
 * over {@link NettyNioAsyncHttpClient}, with {@code endpointOverride} -- so a test can observe
 * which TLS version that stack negotiates.
 *
 * <p>This is the stack used for PUT/GET on S3 (and on GCS when the backend sets {@code
 * useVirtualUrl}). Its builder exposes no way to set enabled TLS protocols, which is why {@code
 * jdk.tls.client.protocols} is the only lever for stage transfers -- the property these probes
 * verify actually reaches it.
 *
 * <p>Trust is deliberately disabled via {@code tlsTrustManagersProvider} so the assertion is about
 * protocol negotiation rather than about the test certificate's chain or hostname.
 */
final class StageSdkTlsProbe {

  private StageSdkTlsProbe() {}

  /**
   * Attempts one request against {@code host:port}. The call always fails -- the target speaks TLS,
   * not S3 -- so callers assert on what the server observed at the TLS layer.
   */
  static void attemptRequest(String host, int port) {
    S3AsyncClient client =
        S3AsyncClient.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("id", "secret")))
            .region(Region.US_WEST_2)
            .endpointOverride(URI.create("https://" + host + ":" + port))
            .forcePathStyle(false)
            .httpClientBuilder(
                NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(1)
                    .connectionTimeout(Duration.ofSeconds(5))
                    .tlsNegotiationTimeout(Duration.ofSeconds(5))
                    .readTimeout(Duration.ofSeconds(5))
                    .writeTimeout(Duration.ofSeconds(5))
                    .tlsTrustManagersProvider(trustAllProvider()))
            .build();

    try {
      client.headObject(b -> b.bucket("test-bucket").key("test-key")).join();
    } catch (Exception expected) {
      // the endpoint is not an S3 service; only the TLS handshake outcome matters
    } finally {
      client.close();
    }
  }

  private static TlsTrustManagersProvider trustAllProvider() {
    return () ->
        new TrustManager[] {
          new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {}

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {}

            @Override
            public X509Certificate[] getAcceptedIssuers() {
              return new X509Certificate[0];
            }
          }
        };
  }
}
