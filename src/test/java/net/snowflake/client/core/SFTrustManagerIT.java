/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.category.TestCategoryCore;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

@Category(TestCategoryCore.class)
public class SFTrustManagerIT extends BaseJDBCTest {
  private static final String[] TARGET_HOSTS = {
    "storage.googleapis.com",
    "ocspssd.us-east-1.snowflakecomputing.com/ocsp/fetch",
    "sfcsupport.snowflakecomputing.com",
    "sfcsupport.us-east-1.snowflakecomputing.com",
    "sfcsupport.eu-central-1.snowflakecomputing.com",
    "sfc-dev1-regression.s3.amazonaws.com",
    "sfc-ds2-customer-stage.s3.amazonaws.com",
    "snowflake.okta.com",
    "sfcdev2.blob.core.windows.net"
  };

  private boolean defaultState;

  @Before
  public void setUp() {
    TelemetryService service = TelemetryService.getInstance();
    service.updateContextForIT(getConnectionParameters());
    defaultState = service.isEnabled();
    service.setNumOfRetryToTriggerTelemetry(3);
    service.enable();
  }

  @After
  public void tearDown() throws InterruptedException {
    TelemetryService service = TelemetryService.getInstance();
    // wait 5 seconds while the service is flushing
    TimeUnit.SECONDS.sleep(5);
    if (defaultState) {
      service.enable();
    } else {
      service.disable();
    }
    System.clearProperty(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED);
    System.clearProperty(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL);
  }

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * OCSP tests for the Snowflake and AWS S3 HTTPS connections.
   *
   * <p>Whatever the default method is used.
   */
  @Test
  public void testOcsp() throws Throwable {
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.TRUE.toString());
    for (String host : TARGET_HOSTS) {
      HttpClient client =
          HttpUtil.buildHttpClient(
              new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED),
              null, // default OCSP response cache file
              false // enable decompression
              );
      accessHost(host, client);
    }
  }

  /**
   * OCSP tests for the Snowflake and AWS S3 HTTPS connections using the file cache.
   *
   * <p>Specifying an non-existing file will force to fetch OCSP response.
   */
  @Test
  public void testOcspWithFileCache() throws Throwable {
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.FALSE.toString());
    File ocspCacheFile = tmpFolder.newFile();
    for (String host : TARGET_HOSTS) {
      HttpClient client =
          HttpUtil.buildHttpClient(
              new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED),
              ocspCacheFile, // a temp OCSP response cache file
              false // enable decompression
              );
      accessHost(host, client);
    }
  }

  /** OCSP tests for the Snowflake and AWS S3 HTTPS connections using the server cache. */
  @Test
  public void testOcspWithServerCache() throws Throwable {
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.TRUE.toString());
    File ocspCacheFile = tmpFolder.newFile();
    for (String host : TARGET_HOSTS) {
      HttpClient client =
          HttpUtil.buildHttpClient(
              new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED),
              ocspCacheFile, // a temp OCSP response cache file
              false // enable decompression
              );
      accessHost(host, client);
    }
  }

  /**
   * OCSP tests for the Snowflake and AWS S3 HTTPS connections without using the server cache. This
   * test should always pass - even with OCSP Outage.
   */
  @Test
  public void testOcspWithoutServerCache() throws Throwable {
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.FALSE.toString());
    File ocspCacheFile = tmpFolder.newFile();
    for (String host : TARGET_HOSTS) {
      HttpClient client =
          HttpUtil.buildHttpClient(
              new HttpClientSettingsKey(OCSPMode.FAIL_OPEN),
              ocspCacheFile, // a temp OCSP response cache file
              false // enable decompression
              );
      accessHost(host, client);
    }
  }

  /** OCSP tests for the Snowflake and AWS S3 HTTPS connections using the server cache. */
  @Test
  public void testInvalidCacheFile() throws Throwable {
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.TRUE.toString());
    // a file under never exists.
    File ocspCacheFile = new File("NEVER_EXISTS", "NEVER_EXISTS");
    String host = TARGET_HOSTS[0];
    HttpClient client =
        HttpUtil.buildHttpClient(
            new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED),
            ocspCacheFile, // a temp OCSP response cache file
            false // enable decompression
            );
    accessHost(host, client);
  }

  private static void accessHost(String host, HttpClient client) throws IOException {
    final int maxRetry = 10;
    int statusCode = -1;
    for (int retry = 0; retry < maxRetry; ++retry) {
      HttpGet httpRequest = new HttpGet(String.format("https://%s:443/", host));
      HttpResponse response = client.execute(httpRequest);
      statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != 503 && statusCode != 504) {
        break;
      }
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException ex) {
        // nop
      }
    }
    assertThat(
        String.format("response code for %s", host),
        statusCode,
        anyOf(equalTo(200), equalTo(403), equalTo(400)));
  }

  /**
   * TODO: we should re-enable this https://snowflakecomputing.atlassian.net/browse/SNOW-146911
   * Revoked certificate test. @Test public void testRevokedCertificate() throws Throwable {
   * System.setProperty(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED,
   * Boolean.TRUE.toString()); File ocspCacheFile = tmpFolder.newFile(); List<X509Certificate>
   * certList = getX509CertificatesFromFile( "revoked_certs.pem"); SFTrustManager sft = new
   * SFTrustManager( OCSPMode.FAIL_OPEN, ocspCacheFile // a temp OCSP response cache file ); int
   * queueSize = TelemetryService.getInstance().size(); try {
   * sft.validateRevocationStatus(certList.toArray(new X509Certificate[0]), "test_host"); fail(); }
   * catch (CertificateException ex) { assertThat(ex.getMessage(), containsString("has been
   * revoked")); if (TelemetryService.getInstance().isDeploymentEnabled()) {
   * assertEquals(TelemetryService.getInstance().size(), queueSize + 1); TelemetryEvent te =
   * TelemetryService.getInstance().peek(); JSONObject values = (JSONObject) te.get("Value"); Object
   * cacheHit = values.get("cacheHit"); assertNotNull(cacheHit); assertEquals("OCSPException",
   * te.get("Name")); assertEquals(SFTrustManager.SF_OCSP_EVENT_TYPE_REVOKED_CERTIFICATE_ERROR,
   * values.get("eventType").toString()); assertNotNull(values.get("sfcPeerHost"));
   * assertNotNull(values.get("certId")); if (cacheHit instanceof Boolean && !((Boolean) cacheHit))
   * { // Only if the cache is not available, no OCSP Responder URL or OCSP request is valid,
   * assertNotNull(values.get("ocspResponderURL")); assertNotNull(values.get("ocspReqBase64")); }
   * assertEquals(OCSPMode.FAIL_OPEN.name(), values.get("ocspMode"));
   * assertNotNull(values.get("cacheEnabled")); assertNotNull(values.get("exceptionStackTrace"));
   * assertNotNull(values.get("exceptionMessage")); } } }
   */

  /**
   * Read certificates from a file.
   *
   * @param filename file name under resources directory
   * @return an array of X509Certificate
   * @throws Throwable raise if any error occurs
   */
  private List<X509Certificate> getX509CertificatesFromFile(String filename) throws Throwable {
    CertificateFactory fact = CertificateFactory.getInstance("X.509");
    List<X509Certificate> certList = new ArrayList<>();
    for (Certificate cert : fact.generateCertificates(getFile(filename))) {
      certList.add((X509Certificate) cert);
    }
    return certList;
  }

  private InputStream getFile(String fileName) throws Throwable {
    ClassLoader classLoader = getClass().getClassLoader();
    URL url = classLoader.getResource(fileName);
    return url != null ? url.openStream() : null;
  }
}
