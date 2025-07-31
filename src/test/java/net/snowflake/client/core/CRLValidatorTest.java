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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
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

@Tag("CORE")
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
  void shouldSkipShortLivedCertificates() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(true) // Allow since we don't have CRL URLs
            .build();
    validator = new CRLValidator(config);

    // Create a short-lived certificate (5 days validity)
    X509Certificate shortLivedCert = certGen.createShortLivedCertificate(5);
    CertificateChain chain = certGen.createSimpleChain();

    List<X509Certificate[]> chains = new ArrayList<>();
    chains.add(new X509Certificate[] {shortLivedCert, chain.intermediateCert, chain.rootCert});

    // Should pass because short-lived certificate is skipped
    boolean result = validator.validateCertificateChains(chains);
    assertTrue(result, "Should pass when chain contains short-lived certificates");
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
            .enableCRLDiskCaching(false)
            .enableCRLInMemoryCaching(false)
            .crlResponseCacheDir("/tmp/test")
            .crlValidityTimeMs(5000)
            .connectionTimeoutMs(10000)
            .readTimeoutMs(15000)
            .build();

    assertEquals(
        CRLValidationConfig.CertRevocationCheckMode.ENABLED, config.getCertRevocationCheckMode());
    assertTrue(config.isAllowCertificatesWithoutCrlUrl());
    assertFalse(config.isEnableCRLDiskCaching());
    assertFalse(config.isEnableCRLInMemoryCaching());
    assertEquals("/tmp/test", config.getCrlResponseCacheDir());
    assertEquals(5000, config.getCrlValidityTimeMs());
    assertEquals(10000, config.getConnectionTimeoutMs());
    assertEquals(15000, config.getReadTimeoutMs());
  }

  @Test
  void shouldUseDefaultConfigValues() {
    CRLValidationConfig config = CRLValidationConfig.builder().build();

    assertEquals(
        CRLValidationConfig.CertRevocationCheckMode.DISABLED, config.getCertRevocationCheckMode());
    assertFalse(config.isAllowCertificatesWithoutCrlUrl());
    assertTrue(config.isEnableCRLDiskCaching());
    assertTrue(config.isEnableCRLInMemoryCaching());
    assertEquals(10L * 24 * 60 * 60 * 1000, config.getCrlValidityTimeMs()); // 10 days
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
      KeyPair keyPair = generateKeyPair();

      long now = System.currentTimeMillis();
      Date notBefore = new Date(now);
      Date notAfter = new Date(now + validityDays * 24L * 60 * 60 * 1000);

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
