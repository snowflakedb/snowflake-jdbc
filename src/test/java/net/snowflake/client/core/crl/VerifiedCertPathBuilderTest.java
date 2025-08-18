package net.snowflake.client.core.crl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.category.TestTags;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
class VerifiedCertPathBuilderTest {

  // Cross-signed scenario path identifiers
  private static final String PATH_INTERMEDIATE_TO_ROOT =
      "intermediateCrossSignedByRoot->rootCASelfSigned";
  private static final String PATH_INTERMEDIATE_TO_OLD_ROOT =
      "intermediateCrossSignedByOldRoot->oldRootCA";

  // Certificate generation constants
  private static final String RSA_ALGORITHM = "RSA";
  private static final String SIGNATURE_ALGORITHM = "SHA256WithRSA";
  private static final String BOUNCY_CASTLE_PROVIDER = "BC";

  private TestCertificateGenerator certGen;

  @BeforeEach
  void setUp() {
    certGen = new TestCertificateGenerator();
  }

  @Test
  void shouldValidateSimpleCertificateChain() throws Exception {
    CertificateChain chain = certGen.createSimpleChain();
    KeyStore trustStore = createInMemoryTrustStore(chain.rootCert);

    VerifiedCertPathBuilder builder = new VerifiedCertPathBuilder(createTrustManager(trustStore));
    X509Certificate[] certChain = {chain.leafCert, chain.intermediateCert, chain.rootCert};
    List<X509Certificate[]> paths = builder.buildAllVerifiedPaths(certChain, "RSA");

    assertFalse(paths.isEmpty(), "Should find at least one valid path");
    assertEquals(1, paths.size(), "Should find exactly one path for simple chain");

    X509Certificate[] path = paths.get(0);
    assertEquals(3, path.length, "Expected path length of 3 (including trust anchor)");

    // Verify path structure: [leaf, intermediate, trust anchor]
    assertEquals(chain.leafCert.getSubjectX500Principal(), path[0].getSubjectX500Principal());
    assertEquals(
        chain.intermediateCert.getSubjectX500Principal(), path[1].getSubjectX500Principal());
    assertEquals(chain.rootCert.getSubjectX500Principal(), path[2].getSubjectX500Principal());

    validateCertificateChainOrder(path);
    assertTrustAnchorInPath(path, chain.rootCert);
  }

  @Test
  void shouldReturnEmptyForEmptyChain() throws Exception {
    // Create a minimal trust store for validation testing
    CertificateChain chain = certGen.createSimpleChain();
    KeyStore trustStore = createInMemoryTrustStore(chain.rootCert);
    VerifiedCertPathBuilder builder = new VerifiedCertPathBuilder(createTrustManager(trustStore));

    assertThrows(
        IllegalArgumentException.class,
        () -> builder.buildAllVerifiedPaths(new X509Certificate[] {}, "RSA"),
        "Empty certificate chain should throw IllegalArgumentException");

    assertThrows(
        IllegalArgumentException.class,
        () -> builder.buildAllVerifiedPaths(null, "RSA"),
        "Null certificate chain should throw IllegalArgumentException");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            builder.buildAllVerifiedPaths(
                new X509Certificate[] {certGen.createSimpleChain().leafCert}, null),
        "Null authType should throw IllegalArgumentException");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            builder.buildAllVerifiedPaths(
                new X509Certificate[] {certGen.createSimpleChain().leafCert}, ""),
        "Empty authType should throw IllegalArgumentException");
  }

  @Test
  void shouldFindAllValidPathsInCrossSignedScenario() throws Exception {
    CrossSignedCertificates certs = certGen.createCrossSignedCertificates();
    KeyStore trustStore = createInMemoryTrustStore(certs.oldRootCA, certs.rootCASelfSigned);
    VerifiedCertPathBuilder builder = new VerifiedCertPathBuilder(createTrustManager(trustStore));
    X509Certificate[] chain = {
      certs.leafCert,
      certs.intermediateCrossSignedByRoot,
      certs.intermediateCrossSignedByOldRoot,
      certs.oldRootCA,
      certs.rootCASelfSigned
    };

    List<X509Certificate[]> paths = builder.buildAllVerifiedPaths(chain, "RSA");

    assertFalse(paths.isEmpty(), "Should find valid paths");
    assertEquals(2, paths.size(), "Should find exactly 2 paths in cross-signed scenario");

    Set<String> foundPairings = validateCrossSignedPaths(paths, certs);
    assertEquals(
        2, foundPairings.size(), "Should find 2 distinct intermediate-trust anchor pairings");
    assertTrue(foundPairings.contains(PATH_INTERMEDIATE_TO_ROOT));
    assertTrue(foundPairings.contains(PATH_INTERMEDIATE_TO_OLD_ROOT));
  }

  @Test
  void shouldValidateComplexCrossSignedChain() throws Exception {
    ComplexCrossSignedCertificates certs = certGen.createComplexCrossSignedChain();
    KeyStore trustStore = createInMemoryTrustStore(certs.selfSignedRoot);

    VerifiedCertPathBuilder builder = new VerifiedCertPathBuilder(createTrustManager(trustStore));
    X509Certificate[] chain = {
      certs.leafCert, certs.intermediate, certs.crossSignedRoot, certs.intermediateRoot
    };

    List<X509Certificate[]> paths = builder.buildAllVerifiedPaths(chain, "RSA");

    assertFalse(paths.isEmpty(), "Should find valid path for complex cross-signed scenario");
    assertEquals(1, paths.size(), "Should find exactly one path");

    X509Certificate[] path = paths.get(0);
    assertEquals(3, path.length, "Expected path length of 3 (including trust anchor)");

    // Verify path structure
    assertEquals(certs.leafCert.getSubjectX500Principal(), path[0].getSubjectX500Principal());
    assertEquals(certs.intermediate.getSubjectX500Principal(), path[1].getSubjectX500Principal());
    assertEquals(certs.selfSignedRoot.getSubjectX500Principal(), path[2].getSubjectX500Principal());

    validateCertificateChainOrder(path);
    assertTrustAnchorInPath(path, certs.selfSignedRoot);
  }

  @Test
  void shouldValidateChainToUltimateRoot() throws Exception {
    ComplexCrossSignedCertificates certs = certGen.createComplexCrossSignedChain();
    KeyStore trustStore = createInMemoryTrustStore(certs.ultimateRoot);

    VerifiedCertPathBuilder builder = new VerifiedCertPathBuilder(createTrustManager(trustStore));
    X509Certificate[] chain = {
      certs.leafCert, certs.intermediate, certs.crossSignedRoot, certs.intermediateRoot
    };

    List<X509Certificate[]> paths = builder.buildAllVerifiedPaths(chain, "RSA");

    assertFalse(paths.isEmpty(), "Should find valid path to ultimate root");
    assertEquals(1, paths.size(), "Should find exactly one path");

    X509Certificate[] path = paths.get(0);
    assertEquals(5, path.length, "Expected path length of 5 (including trust anchor)");

    // Verify complete path structure
    assertEquals(certs.leafCert.getSubjectX500Principal(), path[0].getSubjectX500Principal());
    assertEquals(certs.intermediate.getSubjectX500Principal(), path[1].getSubjectX500Principal());
    assertEquals(
        certs.crossSignedRoot.getSubjectX500Principal(), path[2].getSubjectX500Principal());
    assertEquals(
        certs.intermediateRoot.getSubjectX500Principal(), path[3].getSubjectX500Principal());
    assertEquals(certs.ultimateRoot.getSubjectX500Principal(), path[4].getSubjectX500Principal());

    validateCertificateChainOrder(path);
    assertTrustAnchorInPath(path, certs.ultimateRoot);
  }

  private Set<String> validateCrossSignedPaths(
      List<X509Certificate[]> paths, CrossSignedCertificates certs) {
    Set<String> foundPairings = new HashSet<>();

    for (int i = 0; i < paths.size(); i++) {
      X509Certificate[] path = paths.get(i);
      assertEquals(3, path.length, "Path " + i + " should have length 3");
      assertEquals(certs.leafCert.getSubjectX500Principal(), path[0].getSubjectX500Principal());

      X509Certificate intermediate = path[1];
      X509Certificate trustAnchor = path[2];

      if (intermediate
              .getIssuerX500Principal()
              .equals(certs.rootCASelfSigned.getSubjectX500Principal())
          && trustAnchor
              .getSubjectX500Principal()
              .equals(certs.rootCASelfSigned.getSubjectX500Principal())) {
        foundPairings.add(PATH_INTERMEDIATE_TO_ROOT);
      } else if (intermediate
              .getIssuerX500Principal()
              .equals(certs.oldRootCA.getSubjectX500Principal())
          && trustAnchor
              .getSubjectX500Principal()
              .equals(certs.oldRootCA.getSubjectX500Principal())) {
        foundPairings.add(PATH_INTERMEDIATE_TO_OLD_ROOT);
      } else {
        fail("Unexpected intermediate-trust anchor pairing in path " + i);
      }

      validateCertificateChainOrder(path);
    }

    return foundPairings;
  }

  private void validateCertificateChainOrder(X509Certificate[] path) {
    // Validate issuer-subject relationships (excluding the trust anchor which may be self-signed)
    for (int i = 0; i < path.length - 2; i++) {
      assertEquals(
          path[i].getIssuerX500Principal(),
          path[i + 1].getSubjectX500Principal(),
          "Certificate " + i + " issuer should match certificate " + (i + 1) + " subject");
    }

    // Verify second-to-last certificate is issued by trust anchor
    if (path.length >= 2) {
      assertEquals(
          path[path.length - 2].getIssuerX500Principal(),
          path[path.length - 1].getSubjectX500Principal(),
          "Second-to-last certificate should be issued by the trust anchor");
    }
  }

  private void assertTrustAnchorInPath(X509Certificate[] path, X509Certificate trustAnchor) {
    boolean found = false;
    for (X509Certificate cert : path) {
      if (trustAnchor.getSubjectX500Principal().equals(cert.getSubjectX500Principal())) {
        found = true;
        break;
      }
    }
    assertTrue(
        found, "Trust anchor should be included in the certification path for CRL validation");
  }

  /** Creates an in-memory KeyStore with the provided certificates as trusted CAs. */
  private KeyStore createInMemoryTrustStore(X509Certificate... certificates) throws Exception {
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null, null);

    for (int i = 0; i < certificates.length; i++) {
      trustStore.setCertificateEntry("testroot" + i, certificates[i]);
    }

    return trustStore;
  }

  /** Creates an X509TrustManager using the provided KeyStore. */
  private X509TrustManager createTrustManager(KeyStore trustStore) throws Exception {
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);

    for (javax.net.ssl.TrustManager tm : tmf.getTrustManagers()) {
      if (tm instanceof X509TrustManager) {
        return (X509TrustManager) tm;
      }
    }

    throw new RuntimeException("No X509TrustManager found");
  }

  /** Certificate generator for test scenarios */
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

    CrossSignedCertificates createCrossSignedCertificates() throws Exception {
      KeyPair rootCAKeyPair = generateKeyPair();
      KeyPair oldRootCAKeyPair = generateKeyPair();
      KeyPair intermediateKeyPair = generateKeyPair();
      KeyPair leafKeyPair = generateKeyPair();

      String rootSubject = "CN=Test Root CA " + random.nextInt(10000);
      String oldRootSubject = "CN=Test Old Root CA " + random.nextInt(10000);
      String intermediateSubject = "CN=Test Cross-Signed Intermediate " + random.nextInt(10000);

      X509Certificate rootCASelfSigned =
          createSelfSignedCertificate(rootCAKeyPair, rootSubject, true);
      X509Certificate oldRootCA =
          createSelfSignedCertificate(oldRootCAKeyPair, oldRootSubject, true);

      // Create same intermediate signed by different roots
      X509Certificate intermediateCrossSignedByRoot =
          createSignedCertificate(
              intermediateKeyPair.getPublic(),
              intermediateSubject,
              rootCAKeyPair.getPrivate(),
              rootCASelfSigned,
              true);

      X509Certificate intermediateCrossSignedByOldRoot =
          createSignedCertificate(
              intermediateKeyPair.getPublic(),
              intermediateSubject,
              oldRootCAKeyPair.getPrivate(),
              oldRootCA,
              true);

      X509Certificate leafCert =
          createSignedCertificate(
              leafKeyPair.getPublic(),
              "CN=Test Leaf " + random.nextInt(10000),
              intermediateKeyPair.getPrivate(),
              intermediateCrossSignedByRoot,
              false);

      return new CrossSignedCertificates(
          rootCASelfSigned,
          oldRootCA,
          intermediateCrossSignedByRoot,
          intermediateCrossSignedByOldRoot,
          leafCert);
    }

    ComplexCrossSignedCertificates createComplexCrossSignedChain() throws Exception {
      KeyPair ultimateRootKeyPair = generateKeyPair();
      KeyPair intermediateRootKeyPair = generateKeyPair();
      KeyPair crossSignedRootKeyPair = generateKeyPair();
      KeyPair intermediateKeyPair = generateKeyPair();
      KeyPair leafKeyPair = generateKeyPair();

      X509Certificate ultimateRoot =
          createSelfSignedCertificate(
              ultimateRootKeyPair, "CN=Ultimate Root CA " + random.nextInt(10000), true);

      X509Certificate intermediateRoot =
          createSignedCertificate(
              intermediateRootKeyPair.getPublic(),
              "CN=Intermediate Root CA " + random.nextInt(10000),
              ultimateRootKeyPair.getPrivate(),
              ultimateRoot,
              true);

      String crossSignedSubject = "CN=Cross-Signed Root CA " + random.nextInt(10000);
      X509Certificate crossSignedRoot =
          createSignedCertificate(
              crossSignedRootKeyPair.getPublic(),
              crossSignedSubject,
              intermediateRootKeyPair.getPrivate(),
              intermediateRoot,
              true);

      X509Certificate selfSignedRoot =
          createSelfSignedCertificate(crossSignedRootKeyPair, crossSignedSubject, true);

      X509Certificate intermediate =
          createSignedCertificate(
              intermediateKeyPair.getPublic(),
              "CN=Intermediate CA " + random.nextInt(10000),
              crossSignedRootKeyPair.getPrivate(),
              crossSignedRoot,
              true);

      X509Certificate leafCert =
          createSignedCertificate(
              leafKeyPair.getPublic(),
              "CN=Test Leaf " + random.nextInt(10000),
              intermediateKeyPair.getPrivate(),
              intermediate,
              false);

      return new ComplexCrossSignedCertificates(
          ultimateRoot, intermediateRoot, crossSignedRoot, selfSignedRoot, intermediate, leafCert);
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
      Date notAfter = new Date(now + 365L * 24 * 60 * 60 * 1000);

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

  /** Cross-signed certificate chain holder */
  private static class CrossSignedCertificates {
    final X509Certificate rootCASelfSigned;
    final X509Certificate oldRootCA;
    final X509Certificate intermediateCrossSignedByRoot;
    final X509Certificate intermediateCrossSignedByOldRoot;
    final X509Certificate leafCert;

    CrossSignedCertificates(
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

  /** Complex cross-signed certificate chain holder */
  private static class ComplexCrossSignedCertificates {
    final X509Certificate ultimateRoot;
    final X509Certificate intermediateRoot;
    final X509Certificate crossSignedRoot;
    final X509Certificate selfSignedRoot;
    final X509Certificate intermediate;
    final X509Certificate leafCert;

    ComplexCrossSignedCertificates(
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
