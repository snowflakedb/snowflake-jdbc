package net.snowflake.client.core.crl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.crl.CertificateGeneratorUtil.CertificateChain;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
class CRLValidatorTest {
  private CertificateGeneratorUtil certGen;
  private CloseableHttpClient mockHttpClient;
  private CRLCacheManager mockCacheManager;
  private Telemetry mockTelemetry;

  @BeforeEach
  void setUp() {
    certGen = new CertificateGeneratorUtil();
    mockHttpClient = mock(CloseableHttpClient.class);
    mockCacheManager = mock(CRLCacheManager.class);
    mockTelemetry = mock(Telemetry.class);
  }

  @Test
  void shouldAllowConnectionWhenCRLValidationDisabled() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.DISABLED,
            false,
            mockHttpClient,
            mockCacheManager,
            mockTelemetry);

    assertTrue(validator.validateCertificateChains(chains));
  }

  @Test
  void shouldFailWithNullOrEmptyCertificateChains() {
    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, true, mockHttpClient, mockCacheManager, mockTelemetry);

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

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED,
            false,
            mockHttpClient,
            mockCacheManager,
            mockTelemetry);

    assertFalse(validator.validateCertificateChains(chains));
  }

  @Test
  void shouldAllowCertificatesWithoutCRLUrlsWhenConfigured() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, true, mockHttpClient, mockCacheManager, mockTelemetry);

    assertTrue(validator.validateCertificateChains(chains));
  }

  @Test
  void shouldPassInAdvisoryModeEvenWithErrors() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ADVISORY,
            false,
            mockHttpClient,
            mockCacheManager,
            mockTelemetry);

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

    CRLValidator validator =
        new CRLValidator(
            CertRevocationCheckMode.ENABLED, true, mockHttpClient, mockCacheManager, mockTelemetry);

    assertTrue(
        validator.validateCertificateChains(chains),
        "Should return true when at least one valid chain is found");
  }
}
