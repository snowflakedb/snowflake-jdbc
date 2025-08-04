package net.snowflake.client.core.crl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.Security;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.crl.CertificateGeneratorUtil.CertificateChain;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
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
  private static final CRLValidationConfig CRL_CONFIG_CACHING =
      CRLValidationConfig.builder()
          .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
          .cacheValidityTime(Duration.ofHours(1))
          .build();

  private static final String TEST_CRL_URL = "http://snowflake.com/test.crl";
  private static CertificateGeneratorUtil certGen;

  private CloseableHttpClient mockHttpClient;
  private CloseableHttpResponse mockResponse;
  private HttpEntity mockEntity;
  private CRLCacheManager mockCacheManager;

  @BeforeAll
  static void setUpClass() {
    Security.addProvider(new BouncyCastleProvider());
    certGen = new CertificateGeneratorUtil();
  }

  @BeforeEach
  void setUp() {
    mockHttpClient = mock(CloseableHttpClient.class);
    mockResponse = mock(CloseableHttpResponse.class);
    mockEntity = mock(HttpEntity.class);
    mockCacheManager = mock(CRLCacheManager.class);
  }

  @Test
  void shouldAllowConnectionWhenCRLValidationDisabled() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    CRLValidator validator =
        new CRLValidator(CRL_CONFIG_DISABLED, mockHttpClient, mockCacheManager);

    assertTrue(validator.validateCertificateChains(chains));
  }

  @Test
  void shouldFailWithNullOrEmptyCertificateChains() {
    CRLValidator validator =
        new CRLValidator(CRL_CONFIG_ENABLED_URL_DISALLOWED, mockHttpClient, mockCacheManager);

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
        new CRLValidator(CRL_CONFIG_ENABLED_URL_DISALLOWED, mockHttpClient, mockCacheManager);

    assertFalse(validator.validateCertificateChains(chains));
  }

  @Test
  void shouldAllowCertificatesWithoutCRLUrlsWhenConfigured() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    CRLValidator validator =
        new CRLValidator(CRL_CONFIG_ENABLED_URL_ALLOWED, mockHttpClient, mockCacheManager);

    assertTrue(validator.validateCertificateChains(chains));
  }

  @Test
  void shouldPassInAdvisoryModeEvenWithErrors() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    CRLValidator validator =
        new CRLValidator(CRL_CONFIG_ADVISORY_URL_DISALLOWED, mockHttpClient, mockCacheManager);

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
        new CRLValidator(CRL_CONFIG_ENABLED_URL_ALLOWED, mockHttpClient, mockCacheManager);

    assertTrue(
        validator.validateCertificateChains(chains),
        "Should return true when at least one valid chain is found");
  }

  @Test
  void shouldUseCacheWhenValidCachedCRLExists() throws Exception {
    X509CRL validCrl = createValidCrl(futureDate());
    CRLCacheEntry cacheEntry =
        new CRLCacheEntry(validCrl, Instant.now().minus(30, ChronoUnit.MINUTES));
    when(mockCacheManager.get(TEST_CRL_URL)).thenReturn(cacheEntry);

    boolean result = validateCertificateChain();

    assertTrue(result);
    verify(mockCacheManager).get(TEST_CRL_URL);
    verify(mockHttpClient, never()).execute(any(HttpGet.class));
    verify(mockCacheManager, never()).put(anyString(), any(X509CRL.class), any(Instant.class));
  }

  @Test
  void shouldFetchCRLFromNetworkWhenCacheIsEmpty() throws Exception {
    X509CRL validCrl = createValidCrl();
    setupHttpMockToReturnCRL(validCrl);
    when(mockCacheManager.get(TEST_CRL_URL)).thenReturn(null);

    boolean result = validateCertificateChain();

    assertTrue(result);
    verify(mockCacheManager).get(TEST_CRL_URL);
    verify(mockHttpClient).execute(any(HttpGet.class));
    verify(mockCacheManager).put(eq(TEST_CRL_URL), eq(validCrl), any(Instant.class));
  }

  @Test
  void shouldFetchNewCRLWhenCachedCRLIsExpired() throws Exception {
    X509CRL newValidCrl =
        createCrlWithThisUpdate(Instant.now().plus(1, ChronoUnit.HOURS), futureDate());
    CRLCacheEntry expiredCacheEntry =
        new CRLCacheEntry(createValidCrl(pastDate()), oldDownloadTime());
    setupHttpMockToReturnCRL(newValidCrl);
    when(mockCacheManager.get(TEST_CRL_URL)).thenReturn(expiredCacheEntry);

    boolean result = validateCertificateChain();

    assertTrue(result);
    verify(mockCacheManager).get(TEST_CRL_URL);
    verify(mockHttpClient).execute(any(HttpGet.class));
    verify(mockCacheManager).put(eq(TEST_CRL_URL), eq(newValidCrl), any(Instant.class));
  }

  @Test
  void shouldKeepNewerCachedCRLWhenFetchedCRLIsOlder() throws Exception {
    Date futureDate = futureDate();
    X509CRL newerCrl = createCrlWithThisUpdate(Instant.now(), futureDate);
    X509CRL olderCrl =
        createCrlWithThisUpdate(Instant.now().minus(1, ChronoUnit.HOURS), futureDate);
    CRLCacheEntry cacheEntry = new CRLCacheEntry(newerCrl, expiredDownloadTime());
    setupHttpMockToReturnCRL(olderCrl);
    when(mockCacheManager.get(TEST_CRL_URL)).thenReturn(cacheEntry);

    boolean result = validateCertificateChain();

    assertTrue(result);
    verify(mockCacheManager).get(TEST_CRL_URL);
    verify(mockHttpClient).execute(any(HttpGet.class));
    verify(mockCacheManager, never()).put(anyString(), any(X509CRL.class), any(Instant.class));
  }

  @Test
  void shouldFallbackToCachedCRLWhenNetworkFailsWithValidCache() throws Exception {
    X509CRL validCrl = createValidCrl(futureDate());
    CRLCacheEntry cacheEntry = new CRLCacheEntry(validCrl, expiredDownloadTime());
    when(mockHttpClient.execute(any(HttpGet.class))).thenThrow(new IOException("Network error"));
    when(mockCacheManager.get(TEST_CRL_URL)).thenReturn(cacheEntry);

    boolean result = validateCertificateChain();

    assertTrue(result);
    verify(mockCacheManager).get(TEST_CRL_URL);
    verify(mockHttpClient).execute(any(HttpGet.class));
    verify(mockCacheManager, never()).put(anyString(), any(X509CRL.class), any(Instant.class));
  }

  @Test
  void shouldFailWhenNetworkFailsAndNoCacheExists() throws Exception {
    when(mockHttpClient.execute(any(HttpGet.class))).thenThrow(new IOException("Network error"));
    when(mockCacheManager.get(TEST_CRL_URL)).thenReturn(null);

    boolean result = validateCertificateChain();

    assertFalse(result);
    verify(mockCacheManager).get(TEST_CRL_URL);
    verify(mockHttpClient).execute(any(HttpGet.class));
    verify(mockCacheManager, never()).put(anyString(), any(X509CRL.class), any(Instant.class));
  }

  @Test
  void shouldFailWhenNetworkFailsAndCacheIsExpired() throws Exception {
    X509CRL expiredCrl = createValidCrl(pastDate());
    CRLCacheEntry expiredCacheEntry = new CRLCacheEntry(expiredCrl, oldDownloadTime());
    when(mockHttpClient.execute(any(HttpGet.class))).thenThrow(new IOException("Network error"));
    when(mockCacheManager.get(TEST_CRL_URL)).thenReturn(expiredCacheEntry);

    boolean result = validateCertificateChain();

    assertFalse(result);
    verify(mockCacheManager).get(TEST_CRL_URL);
    verify(mockHttpClient).execute(any(HttpGet.class));
    verify(mockCacheManager, never()).put(anyString(), any(X509CRL.class), any(Instant.class));
  }

  private boolean validateCertificateChain() throws Exception {
    X509Certificate cert =
        certGen.createCertificateWithCRLDistributionPoints(
            "CN=Test Server", Collections.singletonList(TEST_CRL_URL));
    X509Certificate parentCert = certGen.getCACertificate();
    CRLValidator validator = new CRLValidator(CRL_CONFIG_CACHING, mockHttpClient, mockCacheManager);
    List<X509Certificate[]> chains =
        Collections.singletonList(new X509Certificate[] {cert, parentCert});
    return validator.validateCertificateChains(chains);
  }

  private X509CRL createValidCrl() throws Exception {
    return createValidCrl(futureDate());
  }

  private X509CRL createValidCrl(Date nextUpdate) throws Exception {
    byte[] crlBytes = certGen.generateCRL(nextUpdate);
    return certGen.convertBytesToCRL(crlBytes);
  }

  private X509CRL createCrlWithThisUpdate(Instant thisUpdate, Date nextUpdate) throws Exception {
    Date thisUpdateDate = Date.from(thisUpdate);
    byte[] crlBytes = certGen.generateCRLWithThisUpdate(thisUpdateDate, nextUpdate);
    return certGen.convertBytesToCRL(crlBytes);
  }

  private Date futureDate() {
    return Date.from(Instant.now().plus(1, ChronoUnit.DAYS));
  }

  private Date pastDate() {
    return Date.from(Instant.now().minus(1, ChronoUnit.DAYS));
  }

  private Instant oldDownloadTime() {
    return Instant.now().minus(2, ChronoUnit.DAYS);
  }

  private Instant expiredDownloadTime() {
    return Instant.now().minus(Duration.ofHours(1)).minus(1, ChronoUnit.MINUTES);
  }

  private void setupHttpMockToReturnCRL(X509CRL crl) throws Exception {
    byte[] crlBytes = crl.getEncoded();
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(crlBytes));
  }
}
