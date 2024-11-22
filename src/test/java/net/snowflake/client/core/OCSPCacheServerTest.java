package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class OCSPCacheServerTest {

  static class URLProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of(
              "bla-12345.global.snowflakecomputing.com",
              "https://ocspssd-12345.global.snowflakecomputing.com/ocsp/fetch",
              "https://ocspssd-12345.global.snowflakecomputing.com/ocsp/retry"),
          Arguments.of(
              "bla-12345.global.snowflakecomputing.cn",
              "https://ocspssd-12345.global.snowflakecomputing.cn/ocsp/fetch",
              "https://ocspssd-12345.global.snowflakecomputing.cn/ocsp/retry"),
          Arguments.of(
              "bla-12345.global.snowflakecomputing.xyz",
              "https://ocspssd-12345.global.snowflakecomputing.xyz/ocsp/fetch",
              "https://ocspssd-12345.global.snowflakecomputing.xyz/ocsp/retry"),
          Arguments.of(
              "bla-12345.GLOBAL.snowflakecomputing.xyz",
              "https://ocspssd-12345.GLOBAL.snowflakecomputing.xyz/ocsp/fetch",
              "https://ocspssd-12345.GLOBAL.snowflakecomputing.xyz/ocsp/retry"),
          Arguments.of(
              "bla-12345.snowflakecomputing.com",
              "https://ocspssd.snowflakecomputing.com/ocsp/fetch",
              "https://ocspssd.snowflakecomputing.com/ocsp/retry"),
          Arguments.of(
              "bla-12345.snowflakecomputing.cn",
              "https://ocspssd.snowflakecomputing.cn/ocsp/fetch",
              "https://ocspssd.snowflakecomputing.cn/ocsp/retry"),
          Arguments.of(
              "bla-12345.snowflakecomputing.xyz",
              "https://ocspssd.snowflakecomputing.xyz/ocsp/fetch",
              "https://ocspssd.snowflakecomputing.xyz/ocsp/retry"),
          Arguments.of(
              "bla-12345.SNOWFLAKEcomputing.xyz",
              "https://ocspssd.SNOWFLAKEcomputing.xyz/ocsp/fetch",
              "https://ocspssd.SNOWFLAKEcomputing.xyz/ocsp/retry"),
          Arguments.of(
              "s3.amazoncomaws.com",
              "https://ocspssd.snowflakecomputing.com/ocsp/fetch",
              "https://ocspssd.snowflakecomputing.com/ocsp/retry"),
          Arguments.of(
              "s3.amazoncomaws.COM",
              "https://ocspssd.snowflakecomputing.COM/ocsp/fetch",
              "https://ocspssd.snowflakecomputing.COM/ocsp/retry"),
          Arguments.of(
              "s3.amazoncomaws.com.cn",
              "https://ocspssd.snowflakecomputing.cn/ocsp/fetch",
              "https://ocspssd.snowflakecomputing.cn/ocsp/retry"),
          Arguments.of(
              "S3.AMAZONCOMAWS.COM.CN",
              "https://ocspssd.snowflakecomputing.CN/ocsp/fetch",
              "https://ocspssd.snowflakecomputing.CN/ocsp/retry"));
    }
  }

  @ParameterizedTest(name = "For host {0} cache server fetch url should be {1} and retry url {2}")
  @ArgumentsSource(URLProvider.class)
  public void shouldChooseOcspCacheServerUrls(
      String host, String expectedFetchUrl, String expectedRetryUrl) {
    SFTrustManager.OCSPCacheServer ocspCacheServer = new SFTrustManager.OCSPCacheServer();
    ocspCacheServer.resetOCSPResponseCacheServer(host);

    assertEquals(expectedFetchUrl, ocspCacheServer.SF_OCSP_RESPONSE_CACHE_SERVER);
    assertEquals(expectedRetryUrl, ocspCacheServer.SF_OCSP_RESPONSE_RETRY_URL);
  }
}
