package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class OCSPCacheServerTest {

  @Parameterized.Parameters(
      name = "For host {0} cache server fetch url should be {1} and retry url {2}")
  public static Object[][] data() {
    return new Object[][] {
      {
        "bla-12345.global.snowflakecomputing.com",
        "https://ocspssd-12345.global.snowflakecomputing.com/ocsp/fetch",
        "https://ocspssd-12345.global.snowflakecomputing.com/ocsp/retry"
      },
      {
        "bla-12345.global.snowflakecomputing.cn",
        "https://ocspssd-12345.global.snowflakecomputing.cn/ocsp/fetch",
        "https://ocspssd-12345.global.snowflakecomputing.cn/ocsp/retry"
      },
      {
        "bla-12345.global.snowflakecomputing.xyz",
        "https://ocspssd-12345.global.snowflakecomputing.xyz/ocsp/fetch",
        "https://ocspssd-12345.global.snowflakecomputing.xyz/ocsp/retry"
      },
      {
        "bla-12345.GLOBAL.snowflakecomputing.xyz",
        "https://ocspssd-12345.GLOBAL.snowflakecomputing.xyz/ocsp/fetch",
        "https://ocspssd-12345.GLOBAL.snowflakecomputing.xyz/ocsp/retry"
      },
      {
        "bla-12345.snowflakecomputing.com",
        "https://ocspssd.snowflakecomputing.com/ocsp/fetch",
        "https://ocspssd.snowflakecomputing.com/ocsp/retry"
      },
      {
        "bla-12345.snowflakecomputing.cn",
        "https://ocspssd.snowflakecomputing.cn/ocsp/fetch",
        "https://ocspssd.snowflakecomputing.cn/ocsp/retry"
      },
      {
        "bla-12345.snowflakecomputing.xyz",
        "https://ocspssd.snowflakecomputing.xyz/ocsp/fetch",
        "https://ocspssd.snowflakecomputing.xyz/ocsp/retry"
      },
      {
        "bla-12345.SNOWFLAKEcomputing.xyz",
        "https://ocspssd.SNOWFLAKEcomputing.xyz/ocsp/fetch",
        "https://ocspssd.SNOWFLAKEcomputing.xyz/ocsp/retry"
      },
      {
        "s3.amazoncomaws.com",
        "https://ocspssd.snowflakecomputing.com/ocsp/fetch",
        "https://ocspssd.snowflakecomputing.com/ocsp/retry"
      },
      {
        "s3.amazoncomaws.COM",
        "https://ocspssd.snowflakecomputing.COM/ocsp/fetch",
        "https://ocspssd.snowflakecomputing.COM/ocsp/retry"
      },
      {
        "s3.amazoncomaws.com.cn",
        "https://ocspssd.snowflakecomputing.cn/ocsp/fetch",
        "https://ocspssd.snowflakecomputing.cn/ocsp/retry"
      },
      {
        "S3.AMAZONCOMAWS.COM.CN",
        "https://ocspssd.snowflakecomputing.CN/ocsp/fetch",
        "https://ocspssd.snowflakecomputing.CN/ocsp/retry"
      },
    };
  }

  private final String host;
  private final String expectedFetchUrl;
  private final String expectedRetryUrl;

  public OCSPCacheServerTest(String host, String expectedFetchUrl, String expectedRetryUrl) {
    this.host = host;
    this.expectedFetchUrl = expectedFetchUrl;
    this.expectedRetryUrl = expectedRetryUrl;
  }

  @Test
  public void shouldChooseOcspCacheServerUrls() {
    SFTrustManager.OCSPCacheServer ocspCacheServer = new SFTrustManager.OCSPCacheServer();
    ocspCacheServer.resetOCSPResponseCacheServer(host);

    assertEquals(expectedFetchUrl, ocspCacheServer.SF_OCSP_RESPONSE_CACHE_SERVER);
    assertEquals(expectedRetryUrl, ocspCacheServer.SF_OCSP_RESPONSE_RETRY_URL);
  }
}
