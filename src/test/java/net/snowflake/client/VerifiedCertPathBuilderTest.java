package net.snowflake.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive test suite for VerifiedCertPathBuilder that tests path validation with cross-signed
 * certificate scenarios using programmatically created certificates.
 */
@org.junit.jupiter.api.Tag("CORE")
class VerifiedCertPathBuilderTest {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(VerifiedCertPathBuilderTest.class);

  // Constants for certificate path validation
  private static final String INTERMEDIATE_CROSS_SIGNED_BY_ROOT_PAIRING =
      "intermediateCrossSignedByRoot->rootCASelfSigned";
  private static final String INTERMEDIATE_CROSS_SIGNED_BY_OLD_ROOT_PAIRING =
      "intermediateCrossSignedByOldRoot->oldRootCA";

  // Constants for SSL/TLS system properties
  private static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
  private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD =
      "javax.net.ssl.trustStorePassword";
  private static final String JAVAX_NET_SSL_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";

  // Constants for keystore and cryptographic operations
  private static final String KEYSTORE_TYPE_JKS = "JKS";
  private static final String DEFAULT_KEYSTORE_PASSWORD = "changeit";
  private static final String RSA_ALGORITHM = "RSA";
  private static final String SIGNATURE_ALGORITHM = "SHA256WithRSA";
  private static final String BOUNCY_CASTLE_PROVIDER = "BC";

  // Constants for test file operations
  private static final String TEST_TRUSTSTORE_PREFIX = "test-truststore";
  private static final String KEYSTORE_EXTENSION = ".jks";
  private static final String TEST_ROOT_ALIAS_PREFIX = "testroot";

  private TestCertificateGenerator certGen;
  private String originalTrustStore;
  private String originalTrustStorePassword;
  private String originalTrustStoreType;

  @BeforeEach
  void setUp() throws Exception {
    // Initialize BouncyCastle provider for certificate generation
    Security.addProvider(new BouncyCastleProvider());

    // Store original truststore properties
    originalTrustStore = System.getProperty(JAVAX_NET_SSL_TRUST_STORE);
    originalTrustStorePassword = System.getProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD);
    originalTrustStoreType = System.getProperty(JAVAX_NET_SSL_TRUST_STORE_TYPE);

    // Initialize certificate generator
    certGen = new TestCertificateGenerator();
  }

  @AfterEach
  void tearDown() {
    // Restore original truststore properties
    if (originalTrustStore != null) {
      System.setProperty(JAVAX_NET_SSL_TRUST_STORE, originalTrustStore);
    } else {
      System.clearProperty(JAVAX_NET_SSL_TRUST_STORE);
    }

    if (originalTrustStorePassword != null) {
      System.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, originalTrustStorePassword);
    } else {
      System.clearProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD);
    }

    if (originalTrustStoreType != null) {
      System.setProperty(JAVAX_NET_SSL_TRUST_STORE_TYPE, originalTrustStoreType);
    } else {
      System.clearProperty(JAVAX_NET_SSL_TRUST_STORE_TYPE);
    }
  }

  @Test
  @DisplayName("Should validate simple certificate chain")
  void shouldValidateSimpleCertificateChain() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    setUpTrustStore(chain.rootCert);

    VerifiedCertPathBuilder builder = new VerifiedCertPathBuilder();
    X509Certificate[] certChain = {chain.leafCert, chain.intermediateCert, chain.rootCert};
    List<X509Certificate[]> paths = builder.buildAllVerifiedPaths(certChain);

    assertFalse(paths.isEmpty(), "Should find at least one valid path");
    assertEquals(1, paths.size(), "Should find exactly one path for simple chain");

    // Validate the path structure
    X509Certificate[] path = paths.get(0);
    assertTrue(path.length >= 1, "Path should contain at least the end-entity certificate");

    // Expected path: [leafCert, intermediateCert] (trusted root not included)
    assertEquals(2, path.length, "Expected path length of 2 for simple chain");
    assertEquals(
        chain.leafCert.getSubjectDN(),
        path[0].getSubjectDN(),
        "Path[0] should be the leaf certificate");
    assertEquals(
        chain.intermediateCert.getSubjectDN(),
        path[1].getSubjectDN(),
        "Path[1] should be the intermediate certificate");

    // Verify certificate chain relationships for the built path
    validateCertificateChainOrder(path);

    // Verify trust anchor (rootCert) is NOT included in the path
    assertTrustAnchorNotInPath(path, chain.rootCert);

    // Verify the issuer of the last certificate in the path is the trust anchor
    X509Certificate lastCertInPath = path[path.length - 1];
    assertEquals(
        chain.rootCert.getSubjectX500Principal(),
        lastCertInPath.getIssuerX500Principal(),
        "The issuer of the last certificate in the path should be the trust anchor");

    logger.debug("Successfully validated simple certificate chain with path: [leaf, intermediate]");
  }

  @Test
  @DisplayName("Should return empty list for empty certificate chain")
  void shouldReturnEmptyForEmptyChain() throws Exception {
    VerifiedCertPathBuilder builder = new VerifiedCertPathBuilder();

    // Test with empty array
    X509Certificate[] emptyChain = {};
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          builder.buildAllVerifiedPaths(emptyChain);
        },
        "Empty certificate chain should throw IllegalArgumentException");

    // Test with null
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          builder.buildAllVerifiedPaths(null);
        },
        "Null certificate chain should throw IllegalArgumentException");
  }

  @Test
  @DisplayName("Should find all valid paths in cross-signed scenario")
  void shouldFindAllValidPathsInCrossSignedScenario() throws Exception {
    CrossSignedCertificateChain certs = certGen.createCrossSignedCertificates();
    setUpTrustStore(certs.oldRootCA, certs.rootCASelfSigned);

    VerifiedCertPathBuilder builder = new VerifiedCertPathBuilder();

    // Extended certificate set with cross-signed intermediates - theoretically allows 2 paths
    X509Certificate[] extendedChain = {
      certs.leafCert,
      certs.intermediateCrossSignedByRoot,
      certs.intermediateCrossSignedByOldRoot,
      certs.oldRootCA,
      certs.rootCASelfSigned
    };

    // Test comprehensive path discovery
    List<X509Certificate[]> allPaths = builder.buildAllVerifiedPaths(extendedChain);

    assertFalse(allPaths.isEmpty(), "Should find valid paths");

    // Comprehensive path discovery finds both valid paths in cross-signed scenario
    assertEquals(
        2,
        allPaths.size(),
        "Should find exactly 2 paths in cross-signed scenario for comprehensive CRL validation");

    // Track which specific intermediate-trust anchor pairings are found
    Set<String> foundPairings = new HashSet<>();

    // Validate each path with stronger assertions
    for (int i = 0; i < allPaths.size(); i++) {
      X509Certificate[] path = allPaths.get(i);
      assertEquals(2, path.length, "Path " + i + " should have length 2: [leaf, intermediate]");
      assertEquals(
          certs.leafCert.getSubjectDN(),
          path[0].getSubjectDN(),
          "Path " + i + "[0] should be the leaf certificate");

      // Get the intermediate certificate from the path
      X509Certificate intermediate = path[1];

      // Make strong assertions about the specific intermediate-trust anchor pairing
      // Cross-signed certificates have the same subject DN but different issuers
      // So we identify them by their issuer DN
      if (intermediate.getIssuerDN().equals(certs.rootCASelfSigned.getSubjectDN())) {
        // If intermediate is issued by rootCASelfSigned, then it's the cross-signed by root version
        foundPairings.add(INTERMEDIATE_CROSS_SIGNED_BY_ROOT_PAIRING);

        logger.debug("Path {}: uses intermediateCrossSignedByRoot -> rootCASelfSigned", i);

      } else if (intermediate.getIssuerDN().equals(certs.oldRootCA.getSubjectDN())) {
        // If intermediate is issued by oldRootCA, then it's the cross-signed by old root version
        foundPairings.add(INTERMEDIATE_CROSS_SIGNED_BY_OLD_ROOT_PAIRING);

        logger.debug("Path {}: uses intermediateCrossSignedByOldRoot -> oldRootCA", i);

      } else {
        fail(
            "Path "
                + i
                + ": unexpected intermediate certificate with issuer: "
                + intermediate.getIssuerDN());
      }

      // Validate path structure and trust anchor compliance
      validateCertificateChainOrder(path);
      assertTrustAnchorsNotInPath(path, certs.oldRootCA, certs.rootCASelfSigned);
    }

    // Verify that BOTH expected intermediate-trust anchor pairings are found
    assertEquals(
        2,
        foundPairings.size(),
        "Should find exactly 2 distinct intermediate-trust anchor pairings");
    assertTrue(
        foundPairings.contains(INTERMEDIATE_CROSS_SIGNED_BY_ROOT_PAIRING),
        "Should find path: intermediateCrossSignedByRoot -> rootCASelfSigned");
    assertTrue(
        foundPairings.contains(INTERMEDIATE_CROSS_SIGNED_BY_OLD_ROOT_PAIRING),
        "Should find path: intermediateCrossSignedByOldRoot -> oldRootCA");

    logger.debug(
        "Successfully validated comprehensive path discovery: {} paths with {} distinct pairings",
        allPaths.size(),
        foundPairings.size());
  }

  @Test
  @DisplayName("Should validate cross-signed chain to self-signed root")
  void shouldValidateCrossSignedChainToSelfSignedRoot() throws Exception {
    ComplexCrossSignedCertificateChain certs = certGen.createComplexCrossSignedChain();

    // Trust store contains ONLY the self-signed root CA
    // (same subject DN and public key as crossSignedRoot, but self-signed)
    setUpTrustStore(certs.selfSignedRoot);

    VerifiedCertPathBuilder builder = new VerifiedCertPathBuilder();

    // The chain provided includes cross-signed certificates
    // Chain: leafCert -> intermediate -> crossSignedRoot (issued by intermediateRoot) ->
    // intermediateRoot
    X509Certificate[] chain = {
      certs.leafCert, // Leaf certificate
      certs.intermediate, // Intermediate certificate
      certs.crossSignedRoot, // Cross-signed root (issued by intermediate root)
      certs.intermediateRoot // Intermediate root
    };

    List<X509Certificate[]> paths = builder.buildAllVerifiedPaths(chain);

    assertFalse(
        paths.isEmpty(),
        "PKIX should find valid path because cross-signed root in chain has same public key as trusted self-signed version");
    assertEquals(1, paths.size(), "Should find exactly one path for complex cross-signed scenario");

    // Expected path: [leafCert, intermediate] (cross-signed root verified against trusted
    // selfSignedRoot)
    X509Certificate[] path = paths.get(0);
    assertEquals(2, path.length, "Expected path length of 2 for complex cross-signed chain");
    assertEquals(
        certs.leafCert.getSubjectDN(),
        path[0].getSubjectDN(),
        "Path[0] should be the leaf certificate");
    assertEquals(
        certs.intermediate.getSubjectDN(),
        path[1].getSubjectDN(),
        "Path[1] should be the intermediate certificate");

    validateCertificateChainOrder(path);

    // Verify trust anchor (selfSignedRoot) is NOT included in the path
    assertTrustAnchorNotInPath(path, certs.selfSignedRoot);

    // Verify the issuer of the last certificate in the path is the trust anchor
    X509Certificate lastCertInPath = path[path.length - 1];
    assertEquals(
        certs.selfSignedRoot.getSubjectX500Principal(),
        lastCertInPath.getIssuerX500Principal(),
        "The issuer of the last certificate in the path should be the trust anchor (selfSignedRoot)");

    logger.debug(
        "Successfully validated complex cross-signed chain with path: [leaf, intermediate]");
  }

  @Test
  @DisplayName("Should validate cross-signed chain to ultimate root")
  void shouldValidateCrossSignedChainToUltimateRoot() throws Exception {
    ComplexCrossSignedCertificateChain certs = certGen.createComplexCrossSignedChain();

    // Trust store contains ONLY the ultimate root
    // This tests the longest possible certification path
    setUpTrustStore(certs.ultimateRoot);

    VerifiedCertPathBuilder builder = new VerifiedCertPathBuilder();

    // Provide the complete chain including all intermediate certificates
    X509Certificate[] chain = {
      certs.leafCert, // Leaf certificate
      certs.intermediate, // Intermediate certificate
      certs.crossSignedRoot, // Cross-signed root (issued by intermediate root)
      certs.intermediateRoot // Intermediate root (issued by ultimate root)
      // NOTE: ultimateRoot is NOT included - it's a trust anchor, not part of the chain
    };

    List<X509Certificate[]> paths = builder.buildAllVerifiedPaths(chain);

    assertFalse(paths.isEmpty(), "Should find valid path to ultimate root");
    assertEquals(1, paths.size(), "Should find exactly one path to ultimate root");

    // Expected path: [leafCert, intermediate, crossSignedRoot, intermediateRoot]
    // (ultimateRoot is trusted but not included in returned path)
    X509Certificate[] path = paths.get(0);
    assertEquals(4, path.length, "Expected path length of 4 for complete chain to ultimate root");
    assertEquals(
        certs.leafCert.getSubjectDN(),
        path[0].getSubjectDN(),
        "Path[0] should be the leaf certificate");
    assertEquals(
        certs.intermediate.getSubjectDN(),
        path[1].getSubjectDN(),
        "Path[1] should be the intermediate certificate");
    assertEquals(
        certs.crossSignedRoot.getSubjectDN(),
        path[2].getSubjectDN(),
        "Path[2] should be the cross-signed root certificate");
    assertEquals(
        certs.intermediateRoot.getSubjectDN(),
        path[3].getSubjectDN(),
        "Path[3] should be the intermediate root certificate");

    validateCertificateChainOrder(path);

    // Verify trust anchor (ultimateRoot) is NOT included in the path
    assertTrustAnchorNotInPath(path, certs.ultimateRoot);

    // Verify the issuer of the last certificate in the path is the trust anchor
    X509Certificate lastCertInPath = path[path.length - 1];
    assertEquals(
        certs.ultimateRoot.getSubjectX500Principal(),
        lastCertInPath.getIssuerX500Principal(),
        "The issuer of the last certificate in the path should be the trust anchor (ultimateRoot)");

    logger.debug(
        "Successfully validated complete chain to ultimate root with path: [leaf, intermediate, cross-signed-root, intermediate-root]");
  }

  /** Validates that certificates in a path have proper issuer-subject relationships */
  private void validateCertificateChainOrder(X509Certificate[] path) {
    for (int i = 0; i < path.length - 1; i++) {
      X509Certificate current = path[i];
      X509Certificate issuer = path[i + 1];

      assertEquals(
          current.getIssuerDN(),
          issuer.getSubjectDN(),
          String.format("Certificate %d issuer should match certificate %d subject", i, i + 1));
    }
  }

  /**
   * Validates that the trust anchor is NOT included in the certification path. This is correct PKIX
   * behavior - trust anchors should not be part of the returned path.
   */
  private void assertTrustAnchorNotInPath(X509Certificate[] path, X509Certificate trustAnchor) {
    for (X509Certificate cert : path) {
      assertNotEquals(
          trustAnchor.getSubjectDN(),
          cert.getSubjectDN(),
          "Trust anchor should not be included in the certification path (PKIX compliance)");
    }
  }

  /**
   * Validates that none of the trust anchors are included in the certification path. This is
   * correct PKIX behavior - trust anchors should not be part of the returned path.
   */
  private void assertTrustAnchorsNotInPath(
      X509Certificate[] path, X509Certificate... trustAnchors) {
    for (X509Certificate trustAnchor : trustAnchors) {
      assertTrustAnchorNotInPath(path, trustAnchor);
    }
  }

  /** Sets up a custom truststore with the given certificates. */
  private void setUpTrustStore(X509Certificate... certificates) throws Exception {
    KeyStore trustStore = KeyStore.getInstance(KEYSTORE_TYPE_JKS);
    trustStore.load(null, null);

    for (int i = 0; i < certificates.length; i++) {
      String alias = TEST_ROOT_ALIAS_PREFIX + i;
      trustStore.setCertificateEntry(alias, certificates[i]);
    }

    // Save truststore to temporary file
    File tempFile = File.createTempFile(TEST_TRUSTSTORE_PREFIX, KEYSTORE_EXTENSION);
    tempFile.deleteOnExit();

    try (FileOutputStream fos = new FileOutputStream(tempFile)) {
      trustStore.store(fos, DEFAULT_KEYSTORE_PASSWORD.toCharArray());
    }

    // Set system properties
    System.setProperty(JAVAX_NET_SSL_TRUST_STORE, tempFile.getAbsolutePath());
    System.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, DEFAULT_KEYSTORE_PASSWORD);
    System.setProperty(JAVAX_NET_SSL_TRUST_STORE_TYPE, KEYSTORE_TYPE_JKS);
  }

  /** Helper class for generating test certificates using BouncyCastle. */
  private static class TestCertificateGenerator {

    final SecureRandom random = new SecureRandom();

    /** Creates a simple certificate chain: Leaf -> Intermediate -> Root. */
    CertificateChain createSimpleChain() throws Exception {
      // Generate key pairs
      KeyPair rootKeyPair = generateKeyPair();
      KeyPair intermediateKeyPair = generateKeyPair();
      KeyPair leafKeyPair = generateKeyPair();

      // Create root certificate (self-signed)
      X509Certificate rootCert =
          createSelfSignedCertificate(
              rootKeyPair, "CN=Test Root CA " + random.nextInt(10000), true);

      // Create intermediate certificate (signed by root)
      X509Certificate intermediateCert =
          createSignedCertificate(
              intermediateKeyPair.getPublic(),
              "CN=Test Intermediate CA " + random.nextInt(10000),
              rootKeyPair.getPrivate(),
              rootCert,
              true);

      // Create leaf certificate (signed by intermediate)
      X509Certificate leafCert =
          createSignedCertificate(
              leafKeyPair.getPublic(),
              "CN=Test Leaf " + random.nextInt(10000),
              intermediateKeyPair.getPrivate(),
              intermediateCert,
              false);

      return new CertificateChain(rootCert, intermediateCert, leafCert);
    }

    /** Creates a cross-signed certificate chain scenario. */
    CrossSignedCertificateChain createCrossSignedCertificates() throws Exception {
      // Generate key pairs
      KeyPair rootCAKeyPair = generateKeyPair();
      KeyPair oldRootCAKeyPair = generateKeyPair();
      KeyPair intermediateKeyPair = generateKeyPair();
      KeyPair leafKeyPair = generateKeyPair();

      // Use consistent subject names for cross-signed certificates
      String rootSubject = "CN=Test Root CA " + random.nextInt(10000);
      String oldRootSubject = "CN=Test Old Root CA " + random.nextInt(10000);
      String intermediateSubject = "CN=Test Cross-Signed Intermediate " + random.nextInt(10000);
      String leafSubject = "CN=Test Leaf " + random.nextInt(10000);

      // Create root CA certificate (self-signed)
      X509Certificate rootCASelfSigned =
          createSelfSignedCertificate(rootCAKeyPair, rootSubject, true);

      // Create old root CA certificate (self-signed)
      X509Certificate oldRootCA =
          createSelfSignedCertificate(oldRootCAKeyPair, oldRootSubject, true);

      // Create intermediate certificate signed by root CA
      // This represents the same intermediate certificate signed by the new root
      X509Certificate intermediateCrossSignedByRoot =
          createSignedCertificate(
              intermediateKeyPair.getPublic(),
              intermediateSubject,
              rootCAKeyPair.getPrivate(),
              rootCASelfSigned,
              true);

      // Create the same intermediate certificate signed by old root CA
      // This represents the same intermediate certificate signed by the old root
      X509Certificate intermediateCrossSignedByOldRoot =
          createSignedCertificate(
              intermediateKeyPair.getPublic(),
              intermediateSubject,
              oldRootCAKeyPair.getPrivate(),
              oldRootCA,
              true);

      // Create leaf certificate signed by intermediate
      X509Certificate leafCert =
          createSignedCertificate(
              leafKeyPair.getPublic(),
              leafSubject,
              intermediateKeyPair.getPrivate(),
              intermediateCrossSignedByRoot,
              false);

      return new CrossSignedCertificateChain(
          rootCASelfSigned,
          oldRootCA,
          intermediateCrossSignedByRoot,
          intermediateCrossSignedByOldRoot,
          leafCert);
    }

    /** Creates a complex cross-signed certificate chain with multiple trust anchor levels. */
    ComplexCrossSignedCertificateChain createComplexCrossSignedChain() throws Exception {
      // Generate key pairs for the complex cross-signed scenario
      KeyPair ultimateRootKeyPair = generateKeyPair();
      KeyPair intermediateRootKeyPair = generateKeyPair();
      KeyPair crossSignedRootKeyPair = generateKeyPair(); // Same key pair for both versions
      KeyPair intermediateKeyPair = generateKeyPair();
      KeyPair leafKeyPair = generateKeyPair();

      // Generic certificate subjects
      String ultimateRootSubject =
          "CN=Ultimate Root CA " + random.nextInt(10000) + ", O=Test Organization, C=US";
      String intermediateRootSubject =
          "CN=Intermediate Root CA " + random.nextInt(10000) + ", O=Test Organization, C=US";
      String crossSignedRootSubject =
          "CN=Cross-Signed Root CA " + random.nextInt(10000) + ", O=Test Organization, C=US";
      String intermediateSubject =
          "CN=Test Intermediate CA " + random.nextInt(10000) + ", O=Test Organization, C=US";
      String leafSubject = "CN=test.example.com, O=Test Organization, C=US";

      // Create ultimate root (self-signed)
      X509Certificate ultimateRoot =
          createSelfSignedCertificate(ultimateRootKeyPair, ultimateRootSubject, true);

      // Create intermediate root (signed by ultimate root)
      X509Certificate intermediateRoot =
          createSignedCertificate(
              intermediateRootKeyPair.getPublic(),
              intermediateRootSubject,
              ultimateRootKeyPair.getPrivate(),
              ultimateRoot,
              true);

      // Create cross-signed root issued by intermediate root (cross-signed version)
      X509Certificate crossSignedRoot =
          createSignedCertificate(
              crossSignedRootKeyPair.getPublic(),
              crossSignedRootSubject,
              intermediateRootKeyPair.getPrivate(),
              intermediateRoot,
              true);

      // Create cross-signed root self-signed version (for trust store)
      // This has the same subject DN and public key as the cross-signed version
      X509Certificate selfSignedRoot =
          createSelfSignedCertificate(crossSignedRootKeyPair, crossSignedRootSubject, true);

      // Create intermediate certificate (signed by cross-signed root)
      X509Certificate intermediate =
          createSignedCertificate(
              intermediateKeyPair.getPublic(),
              intermediateSubject,
              crossSignedRootKeyPair.getPrivate(),
              crossSignedRoot,
              true);

      // Create leaf certificate (signed by intermediate)
      X509Certificate leafCert =
          createSignedCertificate(
              leafKeyPair.getPublic(),
              leafSubject,
              intermediateKeyPair.getPrivate(),
              intermediate,
              false);

      return new ComplexCrossSignedCertificateChain(
          ultimateRoot, intermediateRoot, crossSignedRoot, selfSignedRoot, intermediate, leafCert);
    }

    KeyPair generateKeyPair() throws Exception {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance(RSA_ALGORITHM);
      keyGen.initialize(2048);
      return keyGen.generateKeyPair();
    }

    X509Certificate createSelfSignedCertificate(KeyPair keyPair, String subjectDN, boolean isCA)
        throws Exception {

      long now = System.currentTimeMillis();
      Date notBefore = new Date(now);
      Date notAfter = new Date(now + 365L * 24 * 60 * 60 * 1000); // 1 year

      X500Name subject = new X500Name(subjectDN);
      X500Name issuer = subject; // Self-signed

      X509v3CertificateBuilder certBuilder =
          new JcaX509v3CertificateBuilder(
              issuer,
              BigInteger.valueOf(random.nextLong()),
              notBefore,
              notAfter,
              subject,
              keyPair.getPublic());

      // Add basic constraints
      certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCA));

      // Add key usage
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
          new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(keyPair.getPrivate());

      X509CertificateHolder certHolder = certBuilder.build(contentSigner);

      return new JcaX509CertificateConverter()
          .setProvider(BOUNCY_CASTLE_PROVIDER)
          .getCertificate(certHolder);
    }

    X509Certificate createSignedCertificate(
        PublicKey publicKey,
        String subjectDN,
        PrivateKey issuerPrivateKey,
        X509Certificate issuerCert,
        boolean isCA)
        throws Exception {

      long now = System.currentTimeMillis();
      Date notBefore = new Date(now);
      Date notAfter = new Date(now + 365L * 24 * 60 * 60 * 1000); // 1 year

      X500Name subject = new X500Name(subjectDN);
      X500Name issuer = new X500Name(issuerCert.getSubjectDN().getName());

      X509v3CertificateBuilder certBuilder =
          new JcaX509v3CertificateBuilder(
              issuer,
              BigInteger.valueOf(random.nextLong()),
              notBefore,
              notAfter,
              subject,
              publicKey);

      // Add basic constraints
      certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCA));

      // Add key usage
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
          new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(issuerPrivateKey);

      X509CertificateHolder certHolder = certBuilder.build(contentSigner);

      return new JcaX509CertificateConverter()
          .setProvider(BOUNCY_CASTLE_PROVIDER)
          .getCertificate(certHolder);
    }
  }

  /** Holds a simple certificate chain. */
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

  /** Holds a cross-signed certificate chain. */
  private static class CrossSignedCertificateChain {
    final X509Certificate rootCASelfSigned;
    final X509Certificate oldRootCA;
    final X509Certificate intermediateCrossSignedByRoot;
    final X509Certificate intermediateCrossSignedByOldRoot;
    final X509Certificate leafCert;

    CrossSignedCertificateChain(
        X509Certificate rootCASelfSigned,
        X509Certificate oldRootCA,
        X509Certificate intermediateCrossSignedByRoot,
        X509Certificate intermediateCrossSignedByOldRoot,
        X509Certificate leafCert) {
      this.rootCASelfSigned = rootCASelfSigned;
      this.oldRootCA = oldRootCA;
      this.intermediateCrossSignedByRoot = intermediateCrossSignedByRoot;
      this.intermediateCrossSignedByOldRoot = intermediateCrossSignedByOldRoot;
      this.leafCert = leafCert;
    }
  }

  /** Holds a complex cross-signed certificate chain with multiple trust anchor levels. */
  private static class ComplexCrossSignedCertificateChain {
    final X509Certificate ultimateRoot;
    final X509Certificate intermediateRoot;
    final X509Certificate crossSignedRoot;
    final X509Certificate selfSignedRoot;
    final X509Certificate intermediate;
    final X509Certificate leafCert;

    ComplexCrossSignedCertificateChain(
        X509Certificate ultimateRoot,
        X509Certificate intermediateRoot,
        X509Certificate crossSignedRoot,
        X509Certificate selfSignedRoot,
        X509Certificate intermediate,
        X509Certificate leafCert) {
      this.ultimateRoot = ultimateRoot;
      this.intermediateRoot = intermediateRoot;
      this.crossSignedRoot = crossSignedRoot;
      this.selfSignedRoot = selfSignedRoot;
      this.intermediate = intermediate;
      this.leafCert = leafCert;
    }
  }
}
