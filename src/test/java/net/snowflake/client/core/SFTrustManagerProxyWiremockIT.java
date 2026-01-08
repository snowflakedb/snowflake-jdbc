package net.snowflake.client.core;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.crl.CertificateGeneratorUtil;
import net.snowflake.client.jdbc.BaseWiremockTest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verifies OCSP cache honor the proxy configured on the connection. Regression coverage for a bug
 * where OCSP HTTP clients were cached only by timeout, causing OCSP requests for subsequent
 * connections to keep using the first proxy port.
 */
@Tag(TestTags.CORE)
public class SFTrustManagerProxyWiremockIT extends BaseWiremockTest {

  private static final String OCSP_CACHE_PATH = "/ocsp_response_cache.json";
  private static Process secondaryWiremock;
  private static final String OCSP_MAPPING_BODY =
      "{\n"
          + "  \"request\": {\"method\": \"GET\", \"urlPattern\": \".*"
          + OCSP_CACHE_PATH
          + ".*\"},\n"
          + "  \"response\": {\"status\": 200, \"jsonBody\": {\"ok\": true}}\n"
          + "}";
  private static int secondaryWiremockHttpPort;
  private static int secondaryWiremockHttpsPort;

  @BeforeAll
  public static void setUpClass() {
    await()
        .alias("wait for wiremock responding")
        .atMost(Duration.ofSeconds(10))
        .until(
            () -> {
              try {
                secondaryWiremockHttpPort = findFreePort();
                secondaryWiremockHttpsPort = findFreePort();
                secondaryWiremock =
                    startWiremockProcess(secondaryWiremockHttpPort, secondaryWiremockHttpsPort);
                waitForWiremockOnPort(secondaryWiremockHttpPort);
                return true;
              } catch (Exception e) {
                logger.warn("Failed to start wiremock, retrying: ", e);
                return false;
              }
            });
  }

  @Test
  public void ocspRequestsUseConfiguredProxyPerConnection() throws Exception {
    String ocspUrl = "http://dummy-host" + OCSP_CACHE_PATH;
    System.setProperty(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL, ocspUrl);
    SFTrustManager.setOCSPResponseCacheServerURL(ocspUrl);
    System.clearProperty("net.snowflake.jdbc.ocsp_activate_new_endpoint");
    X509Certificate[] chain = generateLeafChain();
    addMappingOnPort(OCSP_MAPPING_BODY, wiremockHttpPort);

    // First connection uses proxy #1
    SFTrustManager trustManagerProxyOne =
        new SFTrustManager(
            new HttpClientSettingsKey(
                OCSPMode.FAIL_OPEN, WIREMOCK_HOST, wiremockHttpPort, "", "", "", "http", "", false),
            null);
    trustManagerProxyOne.validateRevocationStatus(chain, WIREMOCK_HOST);

    verifySingleResponderRequest(wiremockHttpPort);

    // Second connection uses proxy #2 – should not reuse the client built for proxy #1.
    addMappingOnPort(OCSP_MAPPING_BODY, secondaryWiremockHttpPort);

    SFTrustManager trustManagerProxyTwo =
        new SFTrustManager(
            new HttpClientSettingsKey(
                OCSPMode.FAIL_OPEN,
                WIREMOCK_HOST,
                secondaryWiremockHttpPort,
                "",
                "",
                "",
                "http",
                "",
                false),
            null);
    trustManagerProxyTwo.validateRevocationStatus(chain, WIREMOCK_HOST);

    verifySingleResponderRequest(wiremockHttpPort);
    verifySingleResponderRequest(secondaryWiremockHttpPort);
  }

  @AfterAll
  public static void stopSecondaryProxy() {
    stopWiremockStandAlone(secondaryWiremock);
  }

  private void addMappingOnPort(String mapping, int port) {
    HttpPost postRequest = createWiremockPostRequest(mapping, "/__admin/mappings", port);
    try (CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = client.execute(postRequest)) {
      assertEquals(201, response.getStatusLine().getStatusCode());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private X509Certificate[] generateLeafChain() throws Exception {
    X509TrustManager jvmTrustManager = getDefaultTrustManager();
    X509Certificate issuer = jvmTrustManager.getAcceptedIssuers()[0];
    CertificateGeneratorUtil util = new CertificateGeneratorUtil();
    X509Certificate leaf = util.createWithIssuer(issuer.getSubjectX500Principal().getName());
    return new X509Certificate[] {leaf};
  }

  private X509TrustManager getDefaultTrustManager() throws Exception {
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init((java.security.KeyStore) null);
    for (TrustManager tm : tmf.getTrustManagers()) {
      if (tm instanceof X509TrustManager) {
        return (X509TrustManager) tm;
      }
    }
    throw new IllegalStateException("No X509TrustManager found");
  }

  private void verifySingleResponderRequest(int adminPort) {
    long actualCount =
        getAllServeEvents(adminPort).stream()
            .filter(event -> event.getRequest() != null && event.getRequest().getUrl() != null)
            .filter(event -> event.getRequest().getUrl().contains(OCSP_CACHE_PATH))
            .count();
    assertEquals(1, actualCount);
  }
}
