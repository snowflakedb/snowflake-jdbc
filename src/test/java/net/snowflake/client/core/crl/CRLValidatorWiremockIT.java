package net.snowflake.client.core.crl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseWiremockTest;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import net.snowflake.client.jdbc.telemetry.TelemetryField;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@Tag(TestTags.CORE)
public class CRLValidatorWiremockIT extends BaseWiremockTest {
  private CertificateGeneratorUtil certGen;
  private CloseableHttpClient httpClient;
  private CRLCacheManager cacheManager;
  private Telemetry telemetry;
  @TempDir private File cacheDir;

  @BeforeEach
  public void setUpTest() throws SnowflakeSQLLoggedException {
    certGen = new CertificateGeneratorUtil();
    httpClient = HttpClients.createDefault();
    cacheManager =
        CRLCacheManager.build(
            CRLCacheConfig.getInMemoryCacheEnabled(),
            CRLCacheConfig.getOnDiskCacheEnabled(),
            Paths.get(cacheDir.getAbsolutePath()),
            CRLCacheConfig.getCrlOnDiskCacheRemovalDelay(),
            CRLCacheConfig.getCacheValidityTime());
    telemetry = Mockito.mock(Telemetry.class);
    resetWiremock();
  }

  @Test
  void shouldValidateNonRevokedCertificateSuccessfully() throws Exception {
    setupCRLMapping("/test-ca.crl", certGen.generateValidCRL(), 200);
    X509Certificate cert =
        certGen.createCertificateWithCRLDistributionPoints(
            "CN=Test Server",
            Arrays.asList("http://localhost:" + wiremockHttpPort + "/test-ca.crl"));
    X509Certificate[] chain = {cert, certGen.getCACertificate()};

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, false, httpClient, cacheManager, telemetry);

    assertTrue(validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));
  }

  @Test
  void shouldFailForRevokedCertificate() throws Exception {
    X509Certificate cert =
        certGen.createCertificateWithCRLDistributionPoints(
            "CN=Revoked Server",
            Arrays.asList("http://localhost:" + wiremockHttpPort + "/test-ca.crl"));
    X509Certificate[] chain = {cert, certGen.getCACertificate()};
    setupCRLMapping(
        "/test-ca.crl", certGen.generateCRLWithRevokedCertificate(cert.getSerialNumber()), 200);

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, false, httpClient, cacheManager, telemetry);

    assertFalse(
        validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));
  }

  @Test
  void shouldAllowRevokedCertificateWhenCRLValidationDisabled() throws Exception {
    X509Certificate revokedCert =
        certGen.createCertificateWithCRLDistributionPoints(
            "CN=Revoked Server (Disabled Mode)",
            Arrays.asList("http://localhost:" + wiremockHttpPort + "/test-ca.crl"));
    X509Certificate[] chain = {revokedCert, certGen.getCACertificate()};
    setupCRLMapping(
        "/test-ca.crl",
        certGen.generateCRLWithRevokedCertificate(revokedCert.getSerialNumber()),
        200);

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.DISABLED, false, httpClient, cacheManager, telemetry);

    assertTrue(validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));
  }

  @Test
  void shouldPassInAdvisoryModeWithCRLErrors() throws Exception {
    setup404CRLMapping("/test-ca.crl");
    X509Certificate cert =
        certGen.createCertificateWithCRLDistributionPoints(
            "CN=Test Server",
            Arrays.asList("http://localhost:" + wiremockHttpPort + "/test-ca.crl"));
    X509Certificate[] chain = {cert, certGen.getCACertificate()};

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.DISABLED, false, httpClient, cacheManager, telemetry);

    assertTrue(validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));
  }

  @Test
  void shouldFailInEnabledModeWithCRLErrors() throws Exception {
    setup404CRLMapping("/test-ca.crl");
    X509Certificate cert =
        certGen.createCertificateWithCRLDistributionPoints(
            "CN=Test Server",
            Arrays.asList("http://localhost:" + wiremockHttpPort + "/test-ca.crl"));
    X509Certificate[] chain = {cert, certGen.getCACertificate()};

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, false, httpClient, cacheManager, telemetry);

    assertFalse(
        validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));
  }

  @Test
  void shouldValidateMultipleChainsAndReturnFirstValid() throws Exception {
    byte[] validCrlContent = certGen.generateValidCRL();
    setupCRLMapping("/valid-ca.crl", validCrlContent, 200);
    setup404CRLMapping("/invalid-ca.crl");

    X509Certificate invalidCert =
        certGen.createCertificateWithCRLDistributionPoints(
            "CN=Invalid Server",
            Arrays.asList("http://localhost:" + wiremockHttpPort + "/invalid-ca.crl"));
    X509Certificate[] invalidChain = {invalidCert, certGen.getCACertificate()};

    X509Certificate validCert =
        certGen.createCertificateWithCRLDistributionPoints(
            "CN=Valid Server",
            Arrays.asList("http://localhost:" + wiremockHttpPort + "/valid-ca.crl"));
    X509Certificate[] validChain = {validCert, certGen.getCACertificate()};

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, false, httpClient, cacheManager, telemetry);

    assertTrue(validator.validateCertificateChains(Arrays.asList(invalidChain, validChain)));
  }

  @Test
  void shouldRejectExpiredCRL() throws Exception {
    byte[] expiredCrlContent = certGen.generateExpiredCRL();
    setupCRLMapping("/expired-ca.crl", expiredCrlContent, 200);

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, false, httpClient, cacheManager, telemetry);

    X509Certificate cert =
        certGen.createCertificateWithCRLDistributionPoints(
            "CN=Test Server",
            Arrays.asList("http://localhost:" + wiremockHttpPort + "/expired-ca.crl"));
    X509Certificate[] chain = {cert, certGen.getCACertificate()};

    List<X509Certificate[]> chains = Arrays.asList(new X509Certificate[][] {chain});

    assertFalse(validator.validateCertificateChains(chains));
  }

  @Test
  void shouldSkipShortLivedCertificates() throws Exception {
    X509Certificate shortLivedCert = certGen.createShortLivedCertificate(5, new Date());
    X509Certificate[] chain = {shortLivedCert, certGen.getCACertificate()};

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, false, httpClient, cacheManager, telemetry);

    assertTrue(validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));
  }

  @Test
  void shouldHandleMultipleCRLDistributionPoints() throws Exception {
    byte[] crlContent = certGen.generateValidCRL();
    setupCRLMapping("/primary-ca.crl", crlContent, 200);
    setupCRLMapping("/backup-ca.crl", crlContent, 200);

    List<String> crlUrls =
        Arrays.asList(
            "http://localhost:" + wiremockHttpPort + "/primary-ca.crl",
            "http://localhost:" + wiremockHttpPort + "/backup-ca.crl");
    X509Certificate cert =
        certGen.createCertificateWithCRLDistributionPoints("CN=Multi-CRL Server", crlUrls);
    X509Certificate[] chain = {cert, certGen.getCACertificate()};

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, false, httpClient, cacheManager, telemetry);

    assertTrue(validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));
  }

  @Test
  void shouldCacheCRLOnFirstRequestAndReuseOnSecond() throws Exception {
    setupCRLMapping("/cached-ca.crl", certGen.generateValidCRL(), 200);
    String crlUrl = "http://localhost:" + wiremockHttpPort + "/cached-ca.crl";
    X509Certificate cert =
        certGen.createCertificateWithCRLDistributionPoints(
            "CN=Cached Server", Arrays.asList(crlUrl));
    X509Certificate[] chain = {cert, certGen.getCACertificate()};

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, false, httpClient, cacheManager, telemetry);

    assertTrue(validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));

    // Remove the WireMock mapping to ensure second request uses cache
    resetWiremock();

    assertTrue(validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));
  }

  @Test
  void shouldEmitCrlTelemetry() throws Exception {
    byte[] crlContent = certGen.generateValidCRL();
    String crlUrl = "http://localhost:" + wiremockHttpPort + "/test-ca.crl";
    setupCRLMapping("/test-ca.crl", crlContent, 200);
    X509Certificate cert =
        certGen.createCertificateWithCRLDistributionPoints(
            "CN=Test Server", Collections.singletonList(crlUrl));
    X509Certificate[] chain = {cert, certGen.getCACertificate()};

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, false, httpClient, cacheManager, telemetry);

    assertTrue(validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));
    ArgumentCaptor<TelemetryData> captor = ArgumentCaptor.forClass(TelemetryData.class);
    Mockito.verify(telemetry).addLogToBatch(captor.capture());
    JsonNode telemetryData = captor.getValue().getMessage();
    assertEquals(TelemetryField.CLIENT_CRL_STATS.toString(), telemetryData.get("type").asText());
    assertEquals(crlUrl, telemetryData.get("client_crl_url").asText());
    assertEquals(crlContent.length, telemetryData.get("client_crl_bytes").asInt());
    assertEquals(0, telemetryData.get("client_revoked_certificates").asInt());
    assertTrue(telemetryData.get("client_time_downloading_crl").asInt() >= 0);
    assertTrue(telemetryData.get("client_time_parsing_crl").asInt() >= 0);
  }

  private void setupCRLMapping(String urlPath, byte[] crlContent, int statusCode) {
    String mappingBody;
    if (statusCode == 200) {
      mappingBody =
          String.format(
              "{\n"
                  + "  \"mappings\": [{\n"
                  + "    \"request\": {\n"
                  + "      \"method\": \"GET\",\n"
                  + "      \"urlPath\": \"%s\"\n"
                  + "    },\n"
                  + "    \"response\": {\n"
                  + "      \"status\": %d,\n"
                  + "      \"headers\": {\n"
                  + "        \"Content-Type\": \"application/pkcs7-mime\"\n"
                  + "      },\n"
                  + "      \"base64Body\": \"%s\"\n"
                  + "    }\n"
                  + "  }]\n"
                  + "}",
              urlPath, statusCode, java.util.Base64.getEncoder().encodeToString(crlContent));
    } else {
      mappingBody =
          String.format(
              "{\n"
                  + "  \"mappings\": [{\n"
                  + "    \"request\": {\n"
                  + "      \"method\": \"GET\",\n"
                  + "      \"urlPath\": \"%s\"\n"
                  + "    },\n"
                  + "    \"response\": {\n"
                  + "      \"status\": %d\n"
                  + "    }\n"
                  + "  }]\n"
                  + "}",
              urlPath, statusCode);
    }

    importMapping(mappingBody);
  }

  private void setup404CRLMapping(String urlPath) {
    setupCRLMapping(urlPath, new byte[0], 404);
  }
}
