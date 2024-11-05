package net.snowflake.client.core;

import static net.snowflake.client.core.SessionUtil.CLIENT_MEMORY_LIMIT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.Base64;
import java.util.Properties;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializable;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SFTrustManagerTest {
  /** Test building OCSP retry URL */
  static String originalRetryUrlPattern;

  @BeforeAll
  public static void saveStaticValues() {
    originalRetryUrlPattern = SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN;
  }

  @AfterAll
  public static void restoreStaticValues() {
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = originalRetryUrlPattern;
  }

  @Test
  public void testBuildRetryURL() throws Exception {
    // private link
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
    SFTrustManager.resetOCSPResponseCacherServerURL(
        "http://ocsp.us-east-1.privatelink.snowflakecomputing.com/"
            + SFTrustManager.CACHE_FILE_NAME);
    assertThat(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN,
        equalTo("http://ocsp.us-east-1.privatelink.snowflakecomputing.com/retry/%s/%s"));

    // private link with port
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
    SFTrustManager.resetOCSPResponseCacherServerURL(
        "http://ocsp.us-east-1.privatelink.snowflakecomputing.com:80/"
            + SFTrustManager.CACHE_FILE_NAME);
    assertThat(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN,
        equalTo("http://ocsp.us-east-1.privatelink.snowflakecomputing.com:80/retry/%s/%s"));

    // non-privatelink
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
    SFTrustManager.resetOCSPResponseCacherServerURL(
        "http://ocsp.snowflakecomputing.com/" + SFTrustManager.CACHE_FILE_NAME);
    assertThat(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN, nullValue());

    // non-privatelink with port
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
    SFTrustManager.resetOCSPResponseCacherServerURL(
        "http://ocsp.snowflakecomputing.com:80/" + SFTrustManager.CACHE_FILE_NAME);
    assertThat(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN, nullValue());

    // default OCSP Cache server URL in specific domain without port
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
    SFTrustManager.resetOCSPResponseCacherServerURL(
        "http://ocsp.snowflakecomputing.cn/" + SFTrustManager.CACHE_FILE_NAME);
    assertThat(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN, nullValue());

    // default OCSP Cache server URL in specific domain with port
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
    SFTrustManager.resetOCSPResponseCacherServerURL(
        "http://ocsp.snowflakecomputing.cn:80/" + SFTrustManager.CACHE_FILE_NAME);
    assertThat(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN, nullValue());
  }

  @Test
  public void testBuildNewRetryURL() {
    try {
      System.setProperty("net.snowflake.jdbc.ocsp_activate_new_endpoint", Boolean.TRUE.toString());

      SFTrustManager tManager =
          new SFTrustManager(
              new HttpClientSettingsKey(OCSPMode.FAIL_OPEN), null // OCSP Cache file custom location
              ); // Use OCSP Cache Server
      tManager.ocspCacheServer.resetOCSPResponseCacheServer("a1.snowflakecomputing.com");
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_CACHE_SERVER,
          equalTo("https://ocspssd.snowflakecomputing.com/ocsp/fetch"));
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_RETRY_URL,
          equalTo("https://ocspssd.snowflakecomputing.com/ocsp/retry"));

      tManager.ocspCacheServer.resetOCSPResponseCacheServer("a1.snowflakecomputing.cn");
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_CACHE_SERVER,
          equalTo("https://ocspssd.snowflakecomputing.cn/ocsp/fetch"));
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_RETRY_URL,
          equalTo("https://ocspssd.snowflakecomputing.cn/ocsp/retry"));

      tManager.ocspCacheServer.resetOCSPResponseCacheServer(
          "a1-12345.global.snowflakecomputing.com");
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_CACHE_SERVER,
          equalTo("https://ocspssd-12345.global.snowflakecomputing.com/ocsp/fetch"));
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_RETRY_URL,
          equalTo("https://ocspssd-12345.global.snowflakecomputing.com/ocsp/retry"));

      tManager.ocspCacheServer.resetOCSPResponseCacheServer(
          "a1-12345.global.snowflakecomputing.cn");
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_CACHE_SERVER,
          equalTo("https://ocspssd-12345.global.snowflakecomputing.cn/ocsp/fetch"));
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_RETRY_URL,
          equalTo("https://ocspssd-12345.global.snowflakecomputing.cn/ocsp/retry"));

      tManager.ocspCacheServer.resetOCSPResponseCacheServer("okta.snowflake.com");
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_CACHE_SERVER,
          equalTo("https://ocspssd.snowflakecomputing.com/ocsp/fetch"));
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_RETRY_URL,
          equalTo("https://ocspssd.snowflakecomputing.com/ocsp/retry"));

      tManager.ocspCacheServer.resetOCSPResponseCacheServer(
          "a1.us-east-1.privatelink.snowflakecomputing.com");
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_CACHE_SERVER,
          equalTo("https://ocspssd.us-east-1.privatelink.snowflakecomputing.com/ocsp/fetch"));
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_RETRY_URL,
          equalTo("https://ocspssd.us-east-1.privatelink.snowflakecomputing.com/ocsp/retry"));

      tManager.ocspCacheServer.resetOCSPResponseCacheServer(
          "a1.us-east-1.privatelink.snowflakecomputing.cn");
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_CACHE_SERVER,
          equalTo("https://ocspssd.us-east-1.privatelink.snowflakecomputing.cn/ocsp/fetch"));
      assertThat(
          tManager.ocspCacheServer.SF_OCSP_RESPONSE_RETRY_URL,
          equalTo("https://ocspssd.us-east-1.privatelink.snowflakecomputing.cn/ocsp/retry"));
    } finally {
      System.clearProperty("net.snowflake.jdbc.ocsp_activate_new_endpoint");
    }
  }

  /**
   * Test resultSetSerializable.getResultSet(ResultSetRetrieveConfig) can work with private link
   * URL.
   *
   * @throws Exception
   */
  @Test
  public void testSnowflakeResultSetSerializable_getResultSet() throws Exception {
    // Create an empty result set serializable object
    SnowflakeResultSetSerializableV1 resultSetSerializable = new SnowflakeResultSetSerializableV1();
    resultSetSerializable.setFirstChunkStringData(
        Base64.getEncoder().encodeToString("".getBytes(StandardCharsets.UTF_8)));
    resultSetSerializable.setChunkFileCount(0);
    resultSetSerializable.getParameters().put(CLIENT_MEMORY_LIMIT, 10);
    resultSetSerializable.setQueryResultFormat(QueryResultFormat.ARROW);

    // Get ResultSet with NON private link URL.
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
    ResultSet rs =
        resultSetSerializable.getResultSet(
            SnowflakeResultSetSerializable.ResultSetRetrieveConfig.Builder.newInstance()
                .setProxyProperties(new Properties())
                .setSfFullURL("https://sfctest0.snowflakecomputing.com")
                .build());
    // For non-private link, do nothing for SFTrustManager
    assertThat(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN, nullValue());

    // Get ResultSet with private link URL.
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
    rs =
        resultSetSerializable.getResultSet(
            SnowflakeResultSetSerializable.ResultSetRetrieveConfig.Builder.newInstance()
                .setProxyProperties(new Properties())
                .setSfFullURL("https://sfctest0.us-west-2.privatelink.snowflakecomputing.com")
                .build());
    // For private link, SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN is reset accordingly.
    assertThat(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN,
        equalTo("http://ocsp.sfctest0.us-west-2.privatelink.snowflakecomputing.com/retry/%s/%s"));
  }
}
