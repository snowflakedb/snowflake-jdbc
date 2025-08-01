package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import net.snowflake.client.category.TestTags;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
class CRLValidatorTest {

  private static final String RSA_ALGORITHM = "RSA";
  private static final String SIGNATURE_ALGORITHM = "SHA256WithRSA";
  private static final String BOUNCY_CASTLE_PROVIDER = "BC";

  private TestCertificateGenerator certGen;
  private CRLValidator validator;

  @BeforeAll
  static void setUpClass() {
    Security.addProvider(new BouncyCastleProvider());
  }

  @BeforeEach
  void setUp() {
    certGen = new TestCertificateGenerator();
  }

  @Test
  void shouldAllowConnectionWhenCRLValidationDisabled() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.DISABLED)
            .build();
    validator = new CRLValidator(config);

    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    boolean result = validator.validateCertificateChains(chains);
    assertTrue(result, "Should allow connection when CRL validation is disabled");
  }

  @Test
  void shouldFailWithNullConfig() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new CRLValidator(null),
        "Should throw IllegalArgumentException with null config");
  }

  @Test
  void shouldFailWithNullOrEmptyCertificateChains() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .build();
    validator = new CRLValidator(config);

    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateCertificateChains(null),
        "Should throw IllegalArgumentException with null certificate chains");

    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateCertificateChains(new ArrayList<>()),
        "Should throw IllegalArgumentException with empty certificate chains");
  }

  @Test
  void shouldSkipShortLivedCertificatesIssuedAfterMarch2024() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(true) // Allow since we don't have CRL URLs
            .build();
    validator = new CRLValidator(config);

    // Create a short-lived certificate (7 days validity) issued after March 15, 2024
    Date issuedDate =
        Date.from(LocalDate.of(2024, 4, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    X509Certificate shortLivedCert = certGen.createShortLivedCertificate(7, issuedDate);
    CertificateChain chain = certGen.createSimpleChain();

    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {shortLivedCert, chain.intermediateCert, chain.rootCert});

    // Should pass because short-lived certificate is skipped
    boolean result = validator.validateCertificateChains(chains);
    assertTrue(
        result, "Should pass when chain contains short-lived certificates issued after March 2024");
  }

  @Test
  void shouldNotSkipLongLivedCertificatesIssuedAfterMarch2024() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false) // Don't allow - will cause validation to run
            .build();
    validator = new CRLValidator(config);

    // Create a long-lived certificate (30 days validity) issued after March 15, 2024
    Date issuedDate =
        Date.from(LocalDate.of(2024, 4, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    X509Certificate longLivedCert = certGen.createShortLivedCertificate(30, issuedDate);
    CertificateChain chain = certGen.createSimpleChain();

    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {longLivedCert, chain.intermediateCert, chain.rootCert});

    // Should fail because long-lived certificate is not skipped and has no CRL URLs
    assertThrows(
        CertificateException.class,
        () -> validator.validateCertificateChains(chains),
        "Should fail when chain contains long-lived certificates without CRL URLs");
  }

  @Test
  void shouldNotSkipCertificatesIssuedBeforeMarch2024() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false) // Don't allow - will cause validation to run
            .build();
    validator = new CRLValidator(config);

    // Create a certificate with 5 days validity issued before March 15, 2024
    Date issuedDate =
        Date.from(LocalDate.of(2024, 2, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    X509Certificate oldCert = certGen.createShortLivedCertificate(5, issuedDate);
    CertificateChain chain = certGen.createSimpleChain();

    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {oldCert, chain.intermediateCert, chain.rootCert});

    // Should fail because certificate issued before March 2024 is not considered short-lived
    assertThrows(
        CertificateException.class,
        () -> validator.validateCertificateChains(chains),
        "Should fail for certificates issued before March 2024 regardless of validity period");
  }

  @Test
  void shouldHandleShortLivedThresholdChangesAfter2026() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(true) // Allow since we don't have CRL URLs
            .build();
    validator = new CRLValidator(config);

    // Create a certificate with 8 days validity issued after March 15, 2026
    Date issuedDate =
        Date.from(LocalDate.of(2026, 4, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    X509Certificate cert = certGen.createShortLivedCertificate(8, issuedDate);
    CertificateChain chain = certGen.createSimpleChain();

    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {cert, chain.intermediateCert, chain.rootCert});

    // 8 days should NOT be considered short-lived after 2026 (threshold is 7 days)
    // But we allow certificates without CRL URLs, so this should pass anyway
    boolean result = validator.validateCertificateChains(chains);
    assertTrue(result, "Should pass due to allowCertificatesWithoutCrlUrl=true");
  }

  @Test
  void shouldSkipShortLivedCertificatesWithin7DaysAfter2026() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(true) // Allow to test just the short-lived logic
            .build();
    validator = new CRLValidator(config);

    // Create a certificate with 6 days validity issued after March 15, 2026
    Date issuedDate =
        Date.from(LocalDate.of(2026, 4, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    X509Certificate shortCert = certGen.createShortLivedCertificate(6, issuedDate);
    CertificateChain chain = certGen.createSimpleChain();

    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {shortCert, chain.intermediateCert, chain.rootCert});

    // Should pass because short-lived certificate is skipped and we allow certs without CRL URLs
    boolean result = validator.validateCertificateChains(chains);
    assertTrue(
        result, "Should pass when short-lived cert is skipped (6 days after 2026 threshold)");
  }

  @Test
  void shouldHandleCertificatesWithoutCRLUrlsInEnabledMode() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false) // Don't allow
            .build();
    validator = new CRLValidator(config);

    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    // Should fail because certificates don't have CRL URLs and allowCertificatesWithoutCrlUrl is
    // false
    assertThrows(
        CertificateException.class,
        () -> validator.validateCertificateChains(chains),
        "Should fail when certificates lack CRL URLs and allowCertificatesWithoutCrlUrl is false");
  }

  @Test
  void shouldAllowCertificatesWithoutCRLUrlsWhenConfigured() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(true) // Allow
            .build();
    validator = new CRLValidator(config);

    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    // Should pass because allowCertificatesWithoutCrlUrl is true
    boolean result = validator.validateCertificateChains(chains);
    assertTrue(result, "Should pass when allowCertificatesWithoutCrlUrl is true");
  }

  @Test
  void shouldPassInAdvisoryModeEvenWithErrors() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ADVISORY)
            .allowCertificatesWithoutCrlUrl(false) // Don't allow - will cause errors
            .build();
    validator = new CRLValidator(config);

    CertificateChain chain = certGen.createSimpleChain();
    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain.leafCert, chain.intermediateCert, chain.rootCert});

    // Should pass in advisory mode even with validation errors
    boolean result = validator.validateCertificateChains(chains);
    assertTrue(result, "Should pass in advisory mode even with validation errors");
  }

  @Test
  void shouldValidateMultipleChainsAndReturnFirstValid() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false) // Don't allow certificates without CRL URLs
            .build();
    validator = new CRLValidator(config);

    CertificateChain chain1 = certGen.createSimpleChain(); // Will have errors (no CRL URLs)
    CertificateChain chain2 = certGen.createSimpleChain(); // Will also have errors (no CRL URLs)

    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {chain1.leafCert, chain1.intermediateCert, chain1.rootCert});
    chains.add(new X509Certificate[] {chain2.leafCert, chain2.intermediateCert, chain2.rootCert});

    // Should fail because no valid chain found (all have missing CRL URLs)
    assertThrows(
        CertificateException.class,
        () -> validator.validateCertificateChains(chains),
        "Should fail when no valid chain is found");
  }

  @Test
  void shouldCreateValidConfigWithBuilder() {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(true)
            .connectionTimeoutMs(10000)
            .readTimeoutMs(15000)
            .build();

    assertEquals(
        CRLValidationConfig.CertRevocationCheckMode.ENABLED, config.getCertRevocationCheckMode());
    assertTrue(config.isAllowCertificatesWithoutCrlUrl());
    assertEquals(10000, config.getConnectionTimeoutMs());
    assertEquals(15000, config.getReadTimeoutMs());
  }

  @Test
  void shouldUseDefaultConfigValues() {
    CRLValidationConfig config = CRLValidationConfig.builder().build();

    assertEquals(
        CRLValidationConfig.CertRevocationCheckMode.DISABLED, config.getCertRevocationCheckMode());
    assertFalse(config.isAllowCertificatesWithoutCrlUrl());
    assertEquals(30000, config.getConnectionTimeoutMs());
    assertEquals(30000, config.getReadTimeoutMs());
  }

  @Test
  void shouldExtractCRLDistributionPointsFromCertificate() throws Exception {
    // Create a certificate with CRL Distribution Points extension
    X509Certificate certWithCDP =
        certGen.createCertificateWithCRLDistributionPoints(
            Arrays.asList(
                "http://crl.example.com/test.crl", "https://backup-crl.example.com/test.crl"));

    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false)
            .build();
    validator = new CRLValidator(config);

    // Use reflection to access the private method for testing
    java.lang.reflect.Method extractMethod =
        CRLValidator.class.getDeclaredMethod("extractCRLDistributionPoints", X509Certificate.class);
    extractMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<String> extractedUrls = (List<String>) extractMethod.invoke(validator, certWithCDP);

    assertEquals(2, extractedUrls.size(), "Should extract 2 CRL URLs");
    assertTrue(extractedUrls.contains("http://crl.example.com/test.crl"));
    assertTrue(extractedUrls.contains("https://backup-crl.example.com/test.crl"));
  }

  @Test
  void shouldCorrectlyIdentifyShortLivedCertificates() throws Exception {
    CRLValidationConfig config = CRLValidationConfig.builder().build();
    validator = new CRLValidator(config);

    // Use reflection to access the private isShortLived method
    java.lang.reflect.Method isShortLivedMethod =
        CRLValidator.class.getDeclaredMethod("isShortLived", X509Certificate.class);
    isShortLivedMethod.setAccessible(true);

    // Test 1: Certificate issued before March 15, 2024 should NOT be considered short-lived
    Date feb2024Date =
        Date.from(LocalDate.of(2024, 2, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    X509Certificate oldCert = certGen.createShortLivedCertificate(5, feb2024Date);
    boolean isShortLived = (Boolean) isShortLivedMethod.invoke(validator, oldCert);
    assertFalse(
        isShortLived,
        "Certificates issued before March 15, 2024 should not be considered short-lived");

    // Test 2: Certificate with 5 days validity issued April 1, 2024 should be short-lived
    Date april2024Date =
        Date.from(LocalDate.of(2024, 4, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    X509Certificate shortCert2024 = certGen.createShortLivedCertificate(5, april2024Date);
    isShortLived = (Boolean) isShortLivedMethod.invoke(validator, shortCert2024);
    assertTrue(isShortLived, "5-day certificate issued in 2024 should be short-lived");

    // Test 3: Certificate with 10 days validity issued April 1, 2024 should be short-lived (within
    // threshold)
    X509Certificate shortCert2024_10days = certGen.createShortLivedCertificate(10, april2024Date);
    isShortLived = (Boolean) isShortLivedMethod.invoke(validator, shortCert2024_10days);
    assertTrue(
        isShortLived,
        "10-day certificate issued in 2024 should be short-lived (within 10-day threshold)");

    // Test 4: Certificate with 15 days validity issued April 1, 2024 should NOT be short-lived
    X509Certificate longCert2024 = certGen.createShortLivedCertificate(15, april2024Date);
    isShortLived = (Boolean) isShortLivedMethod.invoke(validator, longCert2024);
    assertFalse(isShortLived, "15-day certificate issued in 2024 should not be short-lived");

    // Test 5: Certificate with 6 days validity issued April 1, 2026 should be short-lived (7-day
    // threshold)
    Date april2026Date =
        Date.from(LocalDate.of(2026, 4, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    X509Certificate shortCert2026 = certGen.createShortLivedCertificate(6, april2026Date);
    isShortLived = (Boolean) isShortLivedMethod.invoke(validator, shortCert2026);
    assertTrue(isShortLived, "6-day certificate issued after March 2026 should be short-lived");

    // Test 6: Certificate with 8 days validity issued April 1, 2026 should NOT be short-lived
    // (7-day threshold)
    X509Certificate longCert2026 = certGen.createShortLivedCertificate(8, april2026Date);
    isShortLived = (Boolean) isShortLivedMethod.invoke(validator, longCert2026);
    assertFalse(
        isShortLived,
        "8-day certificate issued after March 2026 should not be short-lived (7-day threshold)");

    // Test 7: Edge case - Certificate with exactly 7 days validity issued April 1, 2026 should be
    // short-lived (with margin)
    X509Certificate exactCert2026 = certGen.createShortLivedCertificate(7, april2026Date);
    isShortLived = (Boolean) isShortLivedMethod.invoke(validator, exactCert2026);
    assertTrue(
        isShortLived,
        "7-day certificate issued after March 2026 should be short-lived (with 1-minute margin)");

    // Test 8: Edge case for transition period - Certificate with 10 days validity issued February
    // 1, 2026 (before threshold change)
    Date feb2026Date =
        Date.from(LocalDate.of(2026, 2, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    X509Certificate transitionCert = certGen.createShortLivedCertificate(10, feb2026Date);
    isShortLived = (Boolean) isShortLivedMethod.invoke(validator, transitionCert);
    assertTrue(
        isShortLived,
        "10-day certificate issued before March 2026 should be short-lived (10-day threshold)");
  }

  /** Test certificate generator for CRL validation scenarios */
  private static class TestCertificateGenerator {
    private final SecureRandom random = new SecureRandom();

    CertificateChain createSimpleChain() throws Exception {
      KeyPair rootKeyPair = generateKeyPair();
      KeyPair intermediateKeyPair = generateKeyPair();
      KeyPair leafKeyPair = generateKeyPair();

      X509Certificate rootCert =
          createSelfSignedCertificate(
              rootKeyPair, "CN=Test Root CA " + random.nextInt(10000), true);

      X509Certificate intermediateCert =
          createSignedCertificate(
              intermediateKeyPair.getPublic(),
              "CN=Test Intermediate CA " + random.nextInt(10000),
              rootKeyPair.getPrivate(),
              rootCert,
              true);

      X509Certificate leafCert =
          createSignedCertificate(
              leafKeyPair.getPublic(),
              "CN=Test Leaf " + random.nextInt(10000),
              intermediateKeyPair.getPrivate(),
              intermediateCert,
              false);

      return new CertificateChain(rootCert, intermediateCert, leafCert);
    }

    X509Certificate createShortLivedCertificate(int validityDays) throws Exception {
      return createShortLivedCertificate(validityDays, new Date());
    }

    X509Certificate createShortLivedCertificate(int validityDays, Date issuanceDate)
        throws Exception {
      KeyPair keyPair = generateKeyPair();

      Date notBefore = issuanceDate;
      Date notAfter = new Date(issuanceDate.getTime() + validityDays * 24L * 60 * 60 * 1000);

      X509v3CertificateBuilder certBuilder =
          new JcaX509v3CertificateBuilder(
              new X500Name("CN=Test Short-Lived Root"),
              BigInteger.valueOf(random.nextLong()),
              notBefore,
              notAfter,
              new X500Name("CN=Test Short-Lived " + random.nextInt(10000)),
              keyPair.getPublic());

      certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
      certBuilder.addExtension(
          Extension.keyUsage,
          true,
          new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));

      ContentSigner contentSigner =
          new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(keyPair.getPrivate());
      X509CertificateHolder certHolder = certBuilder.build(contentSigner);

      return new JcaX509CertificateConverter()
          .setProvider(BOUNCY_CASTLE_PROVIDER)
          .getCertificate(certHolder);
    }

    private KeyPair generateKeyPair() throws Exception {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance(RSA_ALGORITHM);
      keyGen.initialize(2048);
      return keyGen.generateKeyPair();
    }

    private X509Certificate createSelfSignedCertificate(
        KeyPair keyPair, String subjectDN, boolean isCA) throws Exception {
      return createCertificate(
          keyPair.getPublic(), subjectDN, keyPair.getPrivate(), subjectDN, isCA);
    }

    private X509Certificate createSignedCertificate(
        PublicKey publicKey,
        String subjectDN,
        PrivateKey issuerPrivateKey,
        X509Certificate issuerCert,
        boolean isCA)
        throws Exception {
      return createCertificate(
          publicKey,
          subjectDN,
          issuerPrivateKey,
          issuerCert.getSubjectX500Principal().getName(),
          isCA);
    }

    private X509Certificate createCertificate(
        PublicKey publicKey,
        String subjectDN,
        PrivateKey signerPrivateKey,
        String issuerDN,
        boolean isCA)
        throws Exception {
      long now = System.currentTimeMillis();
      Date notBefore = new Date(now);
      Date notAfter = new Date(now + 365L * 24 * 60 * 60 * 1000); // 1 year

      X509v3CertificateBuilder certBuilder =
          new JcaX509v3CertificateBuilder(
              new X500Name(issuerDN),
              BigInteger.valueOf(random.nextLong()),
              notBefore,
              notAfter,
              new X500Name(subjectDN),
              publicKey);

      certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCA));

      if (isCA) {
        certBuilder.addExtension(
            Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
      } else {
        certBuilder.addExtension(
            Extension.keyUsage,
            true,
            new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
      }

      ContentSigner contentSigner =
          new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(signerPrivateKey);
      X509CertificateHolder certHolder = certBuilder.build(contentSigner);

      return new JcaX509CertificateConverter()
          .setProvider(BOUNCY_CASTLE_PROVIDER)
          .getCertificate(certHolder);
    }

    X509Certificate createCertificateWithCRLDistributionPoints(List<String> crlUrls)
        throws Exception {
      KeyPair keyPair = generateKeyPair();

      long now = System.currentTimeMillis();
      Date notBefore = new Date(now);
      Date notAfter = new Date(now + 365L * 24 * 60 * 60 * 1000); // 1 year

      X509v3CertificateBuilder certBuilder =
          new JcaX509v3CertificateBuilder(
              new X500Name("CN=Test Root CA " + random.nextInt(10000)),
              BigInteger.valueOf(random.nextLong()),
              notBefore,
              notAfter,
              new X500Name("CN=Test Root CA " + random.nextInt(10000)),
              keyPair.getPublic());

      certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
      certBuilder.addExtension(
          Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));

      // Add CRL Distribution Points extension using the provided URLs
      org.bouncycastle.asn1.x509.GeneralName[] generalNames =
          new org.bouncycastle.asn1.x509.GeneralName[crlUrls.size()];

      for (int i = 0; i < crlUrls.size(); i++) {
        generalNames[i] =
            new org.bouncycastle.asn1.x509.GeneralName(
                org.bouncycastle.asn1.x509.GeneralName.uniformResourceIdentifier, crlUrls.get(i));
      }

      org.bouncycastle.asn1.x509.DistributionPointName dpName =
          new org.bouncycastle.asn1.x509.DistributionPointName(
              org.bouncycastle.asn1.x509.DistributionPointName.FULL_NAME,
              new org.bouncycastle.asn1.x509.GeneralNames(generalNames));

      org.bouncycastle.asn1.x509.DistributionPoint[] distributionPoints = {
        new org.bouncycastle.asn1.x509.DistributionPoint(dpName, null, null)
      };

      org.bouncycastle.asn1.x509.CRLDistPoint crlDistPoint =
          new org.bouncycastle.asn1.x509.CRLDistPoint(distributionPoints);
      certBuilder.addExtension(Extension.cRLDistributionPoints, true, crlDistPoint);

      ContentSigner contentSigner =
          new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(keyPair.getPrivate());
      X509CertificateHolder certHolder = certBuilder.build(contentSigner);

      return new JcaX509CertificateConverter()
          .setProvider(BOUNCY_CASTLE_PROVIDER)
          .getCertificate(certHolder);
    }
  }

  /** Simple certificate chain holder */
  private static class CertificateChain {
    final X509Certificate rootCert;
    final X509Certificate intermediateCert;
    final X509Certificate leafCert;

    CertificateChain(
        X509Certificate rootCert, X509Certificate intermediateCert, X509Certificate leafCert) {
      this.rootCert = rootCert;
      this.intermediateCert = intermediateCert;
      this.leafCert = leafCert;
    }
  }
}
