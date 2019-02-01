package net.snowflake.client.core;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class SFTrustManagerTest
{
  /**
   * Test building OCSP retry URL
   */
  @Test
  public void testBuildRetryURL()
  {
    try
    {
      // private link
      System.clearProperty("net.snowflake.jdbc.ssd_support_enabled");
      SFTrustManager.ssdManager = new SSDManager();
      SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
      SFTrustManager.resetOCSPResponseCacherServerURL(
          "http://ocsp.us-east-1.privatelink.snowflakecomputing.com/" +
              SFTrustManager.CACHE_FILE_NAME);
      assertThat(
          SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN,
          equalTo("http://ocsp.us-east-1.privatelink.snowflakecomputing.com/retry/%s/%s"));

      // private link with port
      System.clearProperty("net.snowflake.jdbc.ssd_support_enabled");
      SFTrustManager.ssdManager = new SSDManager();
      SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
      SFTrustManager.resetOCSPResponseCacherServerURL(
          "http://ocsp.us-east-1.privatelink.snowflakecomputing.com:80/" +
              SFTrustManager.CACHE_FILE_NAME);
      assertThat(
          SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN,
          equalTo("http://ocsp.us-east-1.privatelink.snowflakecomputing.com:80/retry/%s/%s"));

      // non-privatelink
      System.clearProperty("net.snowflake.jdbc.ssd_support_enabled");
      SFTrustManager.ssdManager = new SSDManager();
      SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
      SFTrustManager.resetOCSPResponseCacherServerURL(
          "http://ocsp.snowflakecomputing.com/" +
              SFTrustManager.CACHE_FILE_NAME);
      assertThat(
          SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN, nullValue());

      // non-privatelink with port
      System.clearProperty("net.snowflake.jdbc.ssd_support_enabled");
      SFTrustManager.ssdManager = new SSDManager();
      SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
      SFTrustManager.resetOCSPResponseCacherServerURL(
          "http://ocsp.snowflakecomputing.com:80/" +
              SFTrustManager.CACHE_FILE_NAME);
      assertThat(
          SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN, nullValue());

      // SSD enabled, privatelink
      System.setProperty("net.snowflake.jdbc.ssd_support_enabled", "true");
      SFTrustManager.ssdManager = new SSDManager();
      SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
      SFTrustManager.resetOCSPResponseCacherServerURL(
          "http://ocsp.us-east-1.privatelink.snowflakecomputing.com/" +
              SFTrustManager.CACHE_FILE_NAME);
      assertThat(
          SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN,
          equalTo("http://ocsp.us-east-1.privatelink.snowflakecomputing.com/retry"));

      // SSD enabled, privatelink with port
      System.setProperty("net.snowflake.jdbc.ssd_support_enabled", "true");
      SFTrustManager.ssdManager = new SSDManager();
      SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
      SFTrustManager.resetOCSPResponseCacherServerURL(
          "http://ocsp.us-east-1.privatelink.snowflakecomputing.com:80/" +
              SFTrustManager.CACHE_FILE_NAME);
      assertThat(
          SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN,
          equalTo("http://ocsp.us-east-1.privatelink.snowflakecomputing.com:80/retry"));

      // SSD enabled, non-privatelink
      System.setProperty("net.snowflake.jdbc.ssd_support_enabled", "true");
      SFTrustManager.ssdManager = new SSDManager();
      SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
      SFTrustManager.resetOCSPResponseCacherServerURL(
          "http://ocsp.snowflakecomputing.com/" +
              SFTrustManager.CACHE_FILE_NAME);
      assertThat(
          SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN,
          equalTo("http://ocsp.snowflakecomputing.com/retry"));

      // SSD enabled, non-privatelink with port
      System.setProperty("net.snowflake.jdbc.ssd_support_enabled", "true");
      SFTrustManager.ssdManager = new SSDManager();
      SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
      SFTrustManager.resetOCSPResponseCacherServerURL(
          "http://ocsp.snowflakecomputing.com:80/" +
              SFTrustManager.CACHE_FILE_NAME);
      // no port will be included but always the default URL.
      assertThat(
          SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN,
          equalTo("http://ocsp.snowflakecomputing.com/retry"));
    }
    finally
    {
      System.clearProperty("net.snowflake.jdbc.ssd_support_enabled");

    }
  }
}
