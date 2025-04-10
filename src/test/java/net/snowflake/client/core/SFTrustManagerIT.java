package net.snowflake.client.core;

import static net.snowflake.client.core.SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
import net.snowflake.client.SystemPropertyOverrider;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;

@Tag(TestTags.CORE)
public class SFTrustManagerIT extends BaseJDBCTest {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFTrustManagerIT.class);

  private static class HostProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          // this host generates many "SSLHandshake Certificate Revocation
          // check failed. Could not retrieve OCSP Response." when running in parallel CI builds
          // Arguments.of("storage.googleapis.com"),
          Arguments.of("ocspssd.us-east-1.snowflakecomputing.com/ocsp/fetch"),
          Arguments.of("sfcsupport.snowflakecomputing.com"),
          Arguments.of("sfcsupport.us-east-1.snowflakecomputing.com"),
          Arguments.of("sfcsupport.eu-central-1.snowflakecomputing.com"),
          Arguments.of("sfc-dev1-regression.s3.amazonaws.com"),
          Arguments.of("sfc-ds2-customer-stage.s3.amazonaws.com"),
          Arguments.of("snowflake.okta.com"),
          Arguments.of("sfcdev2.blob.core.windows.net"));
    }
  }

  private boolean defaultState;

  @BeforeEach
  public void setUp() {
    TelemetryService service = TelemetryService.getInstance();
    service.updateContextForIT(getConnectionParameters());
    defaultState = service.isEnabled();
    service.setNumOfRetryToTriggerTelemetry(3);
    service.enable();
  }

  @AfterEach
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

  @TempDir File tmpFolder;

  /**
   * OCSP tests for the Snowflake and AWS S3 HTTPS connections.
   *
   * <p>Whatever the default method is used.
   */
  @ParameterizedTest
  @ArgumentsSource(HostProvider.class)
  public void testOcsp(String host) throws Throwable {
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.TRUE.toString());
    // this initialization normally happens on first call
    SFTrustManager.setOCSPResponseCacheServerURL(String.format("http://%s", host));
    HttpClient client =
        HttpUtil.buildHttpClient(
            new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED),
            null, // default OCSP response cache file
            false // enable decompression
            );
    accessHost(host, client);
  }

  /**
   * OCSP tests for the Snowflake and AWS S3 HTTPS connections using the file cache.
   *
   * <p>Specifying an non-existing file will force to fetch OCSP response.
   */
  @ParameterizedTest
  @ArgumentsSource(HostProvider.class)
  public void testOcspWithFileCache(String host) throws Throwable {
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.FALSE.toString());
    File ocspCacheFile = new File(tmpFolder, "ocsp-cache");
    ocspCacheFile.createNewFile();
    HttpClient client =
        HttpUtil.buildHttpClient(
            new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED),
            ocspCacheFile, // a temp OCSP response cache file
            false // enable decompression
            );
    accessHost(host, client);
  }

  /** OCSP tests for the Snowflake and AWS S3 HTTPS connections using the server cache. */
  @ParameterizedTest
  @ArgumentsSource(HostProvider.class)
  public void testOcspWithServerCache(String host) throws Throwable {
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.TRUE.toString());
    File ocspCacheFile = new File(tmpFolder, "ocsp-cache");
    ocspCacheFile.createNewFile();
    HttpClient client =
        HttpUtil.buildHttpClient(
            new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED),
            ocspCacheFile, // a temp OCSP response cache file
            false // enable decompression
            );
    accessHost(host, client);
  }

  /**
   * OCSP tests for the Snowflake and AWS S3 HTTPS connections without using the server cache. This
   * test should always pass - even with OCSP Outage.
   */
  @ParameterizedTest
  @ArgumentsSource(HostProvider.class)
  public void testOcspWithoutServerCache(String host) throws Throwable {
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.FALSE.toString());
    File ocspCacheFile = new File(tmpFolder, "ocsp-cache");
    ocspCacheFile.createNewFile();
    HttpClient client =
        HttpUtil.buildHttpClient(
            new HttpClientSettingsKey(OCSPMode.FAIL_OPEN),
            ocspCacheFile, // a temp OCSP response cache file
            false // enable decompression
            );
    accessHost(host, client);
  }

  /** OCSP tests for the Snowflake and AWS S3 HTTPS connections using the server cache. */
  @ParameterizedTest
  @ArgumentsSource(HostProvider.class)
  public void testInvalidCacheFile(String host) throws Throwable {
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.TRUE.toString());
    // a file under never exists.
    File ocspCacheFile = new File("NEVER_EXISTS", "NEVER_EXISTS");
    HttpClient client =
        HttpUtil.buildHttpClient(
            new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED),
            ocspCacheFile, // a temp OCSP response cache file
            false // enable decompression
            );
    accessHost(host, client);
  }

  private static void accessHost(String host, HttpClient client)
      throws IOException, InterruptedException {
    HttpResponse response = executeWithRetries(host, client);

    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> response.getStatusLine().getStatusCode(), not(equalTo(-1)));

    assertThat(
        String.format("response code for %s", host),
        response.getStatusLine().getStatusCode(),
        anyOf(equalTo(200), equalTo(400), equalTo(403), equalTo(404), equalTo(513)));
  }

  private static HttpResponse executeWithRetries(String host, HttpClient client)
      throws IOException, InterruptedException {
    // There is one host that causes SSLHandshakeException very often - let's retry
    int maxRetries = host.equals("storage.googleapis.com") ? 5 : 0;
    int retries = 0;
    HttpGet httpRequest = new HttpGet(String.format("https://%s:443/", host));
    while (true) {
      try {
        return client.execute(httpRequest);
      } catch (SSLHandshakeException e) {
        logger.warn("SSL handshake failed (host = {}, retries={}}", host, retries, e);
        ++retries;
        if (retries >= maxRetries) {
          throw e;
        }
        Thread.sleep(retries * 1000);
      }
    }
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

  @ParameterizedTest
  @CsvSource({
    "jdbc:snowflake://someaccount.snowflakecomputing.com:443,http://ocsp.snowflakecomputing.com/ocsp_response_cache.json",
    "jdbc:snowflake://someaccount.snowflakecomputing.cn:443,http://ocsp.snowflakecomputing.cn/ocsp_response_cache.json",
  })
  void testOCSPCacheServerUrlWithoutProxy(String sfHost, String ocspHost) throws Exception {
    Properties props = new Properties();
    props.setProperty(SFSessionProperty.USER.getPropertyKey(), "testUser");
    props.setProperty(SFSessionProperty.PASSWORD.getPropertyKey(), "testPassword");
    props.setProperty(SFSessionProperty.LOGIN_TIMEOUT.getPropertyKey(), "1");
    try {
      new SnowflakeConnectionV1(sfHost, props);
    } catch (Exception e) {
      // do nothing, we don't want to connect, just check the value below
    }
    assertEquals(SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE, ocspHost);
  }

  @ParameterizedTest
  @CsvSource({
    "jdbc:snowflake://someaccount.snowflakecomputing.com:443,http://ocsp.snowflakecomputing.com/ocsp_response_cache.json",
    "jdbc:snowflake://someaccount.snowflakecomputing.cn:443,http://ocsp.snowflakecomputing.cn/ocsp_response_cache.json",
  })
  void testOCSPCacheServerUrlWithProxy(String sfHost, String ocspHost) {
    SystemPropertyOverrider useProxyOverrider =
        new SystemPropertyOverrider("http.useProxy", "true");
    SystemPropertyOverrider proxyHostOverrider =
        new SystemPropertyOverrider("http.proxyHost", "localhost");
    SystemPropertyOverrider proxyPortOverrider =
        new SystemPropertyOverrider("http.proxyPort", "8080");
    try {
      Properties props = new Properties();
      props.setProperty(SFSessionProperty.USER.getPropertyKey(), "testUser");
      props.setProperty(SFSessionProperty.PASSWORD.getPropertyKey(), "testPassword");
      props.setProperty(SFSessionProperty.LOGIN_TIMEOUT.getPropertyKey(), "1");
      try {
        new SnowflakeConnectionV1(sfHost, props);
      } catch (Exception e) {
        // do nothing, we don't want to connect, just check the value below
      }
      assertEquals(SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE, ocspHost);
    } finally {
      Arrays.asList(useProxyOverrider, proxyHostOverrider, proxyPortOverrider)
          .forEach(SystemPropertyOverrider::rollback);
    }
  }

  @BeforeEach
  @AfterEach
  void cleanup() {
    SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE = null;
  }
}
