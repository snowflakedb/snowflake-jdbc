package net.snowflake.client.core.crl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseWiremockTest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class CRLValidatorWiremockIT extends BaseWiremockTest {
  private static final CRLValidationConfig CRL_CONFIG_ENABLED =
      CRLValidationConfig.builder()
          .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
          .build();
  private static final CRLValidationConfig CRL_CONFIG_DISABLED =
      CRLValidationConfig.builder()
          .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.DISABLED)
          .build();
  private static final CRLValidationConfig CRL_CONFIG_ADVISORY =
      CRLValidationConfig.builder()
          .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ADVISORY)
          .build();

  private CertificateGeneratorUtil certGen;
  private CloseableHttpClient httpClient;
  private CRLCacheManager cacheManager;

  @BeforeEach
  public void setUpTest() {
    certGen = new CertificateGeneratorUtil();
    httpClient = HttpClients.createDefault();
    cacheManager = CRLCacheManager.fromConfig(CRL_CONFIG_ENABLED);
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

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED, httpClient, cacheManager);

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

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED, httpClient, cacheManager);

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

    CRLValidator validator = new CRLValidator(CRL_CONFIG_DISABLED, httpClient, cacheManager);

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

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ADVISORY, httpClient, cacheManager);

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

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED, httpClient, cacheManager);

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

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED, httpClient, cacheManager);

    assertTrue(validator.validateCertificateChains(Arrays.asList(invalidChain, validChain)));
  }

  @Test
  void shouldRejectExpiredCRL() throws Exception {
    byte[] expiredCrlContent = certGen.generateExpiredCRL();
    setupCRLMapping("/expired-ca.crl", expiredCrlContent, 200);

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED, httpClient, cacheManager);

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

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED, httpClient, cacheManager);

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

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED, httpClient, cacheManager);

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

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED, httpClient, cacheManager);

    assertTrue(validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));

    // Remove the WireMock mapping to ensure second request uses cache
    resetWiremock();

    assertTrue(validator.validateCertificateChains(Arrays.asList(new X509Certificate[][] {chain})));
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
