package net.snowflake.client.core.crl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.security.Security;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.crl.CertificateGeneratorUtil.CertificateChain;
import org.apache.http.impl.client.CloseableHttpClient;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
class CRLValidatorTest {
  private static final CRLValidationConfig CRL_CONFIG_DISABLED =
      CRLValidationConfig.builder()
          .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.DISABLED)
          .build();
  private static final CRLValidationConfig CRL_CONFIG_ENABLED_URL_DISALLOWED =
      CRLValidationConfig.builder()
          .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
          .build();
  private static final CRLValidationConfig CRL_CONFIG_ENABLED_URL_ALLOWED =
      CRLValidationConfig.builder()
          .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
          .allowCertificatesWithoutCrlUrl(true)
          .build();
  private static final CRLValidationConfig CRL_CONFIG_ADVISORY_URL_DISALLOWED =
      CRLValidationConfig.builder()
          .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ADVISORY)
          .build();

  private CertificateGeneratorUtil certGen;
  private CloseableHttpClient mockHttpClient;

  @BeforeAll
  static void setUpClass() {
    Security.addProvider(new BouncyCastleProvider());
  }

  @BeforeEach
  void setUp() {
    certGen = new CertificateGeneratorUtil();
    mockHttpClient = mock(CloseableHttpClient.class);
  }

  @Test
  void shouldAllowConnectionWhenCRLValidationDisabled() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    CRLValidator validator = new CRLValidator(CRL_CONFIG_DISABLED, mockHttpClient);

    assertTrue(validator.validateCertificateChains(chains));
  }

  @Test
  void shouldFailWithNullOrEmptyCertificateChains() {
    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED_URL_DISALLOWED, mockHttpClient);

    assertThrows(IllegalArgumentException.class, () -> validator.validateCertificateChains(null));

    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateCertificateChains(new ArrayList<>()));
  }

  @Test
  void shouldHandleCertificatesWithoutCRLUrlsInEnabledMode() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED_URL_DISALLOWED, mockHttpClient);

    assertFalse(validator.validateCertificateChains(chains));
  }

  @Test
  void shouldAllowCertificatesWithoutCRLUrlsWhenConfigured() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED_URL_ALLOWED, mockHttpClient);

    assertTrue(validator.validateCertificateChains(chains));
  }

  @Test
  void shouldPassInAdvisoryModeEvenWithErrors() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ADVISORY_URL_DISALLOWED, mockHttpClient);

    assertTrue(validator.validateCertificateChains(chains));
  }

  @Test
  void shouldValidateMultipleChainsAndReturnFirstValid() throws Exception {

    Date beforeMarch2024 =
        Date.from(LocalDate.of(2024, 2, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    X509Certificate invalidCert =
        certGen.createShortLivedCertificate(5, beforeMarch2024); // Not considered short-lived
    CertificateChain validChain = certGen.createSimpleChain();

    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(
        new X509Certificate[] {invalidCert, validChain.intermediateCert, validChain.rootCert});
    chains.add(
        new X509Certificate[] {
          validChain.leafCert, validChain.intermediateCert, validChain.rootCert
        });

    CRLValidator validator = new CRLValidator(CRL_CONFIG_ENABLED_URL_ALLOWED, mockHttpClient);

    assertTrue(
        validator.validateCertificateChains(chains),
        "Should return true when at least one valid chain is found");
  }
}
