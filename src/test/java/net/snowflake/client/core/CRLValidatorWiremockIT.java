package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseWiremockTest;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.CRLDistPoint;
import org.bouncycastle.asn1.x509.DistributionPoint;
import org.bouncycastle.asn1.x509.DistributionPointName;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CRLConverter;
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
public class CRLValidatorWiremockIT extends BaseWiremockTest {

  private static final String RSA_ALGORITHM = "RSA";
  private static final String SIGNATURE_ALGORITHM = "SHA256WithRSA";
  private static final String BOUNCY_CASTLE_PROVIDER = "BC";

  private TestCertificateAuthority testCA;
  private CRLValidator validator;

  @BeforeAll
  public static void setUpClassCRL() {
    Security.addProvider(new BouncyCastleProvider());
  }

  @BeforeEach
  public void setUpTest() throws Exception {
    testCA = new TestCertificateAuthority();
    resetWiremock();
  }

  @Test
  void shouldValidateNonRevokedCertificateSuccessfully() throws Exception {
    // Setup WireMock to serve valid CRL
    byte[] crlContent = testCA.generateValidCRL();
    setupCRLMapping("/test-ca.crl", crlContent, 200);

    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false)
            .build();
    validator = new CRLValidator(config);

    // Create certificate with CRL distribution point pointing to WireMock
    X509Certificate cert =
        testCA.createCertificateWithCRLDistributionPoint(
            "CN=Test Server", "http://localhost:" + wiremockHttpPort + "/test-ca.crl");
    X509Certificate[] chain = {cert, testCA.getCACertificate()};

    List<X509Certificate[]> chains = Arrays.asList(new X509Certificate[][] {chain});
    boolean result = validator.validateCertificateChains(chains);

    assertTrue(result, "Should validate non-revoked certificate successfully");
  }

  @Test
  void shouldFailForRevokedCertificate() throws Exception {
    // Create a certificate and add it to revoked list
    X509Certificate cert =
        testCA.createCertificateWithCRLDistributionPoint(
            "CN=Revoked Server", "http://localhost:" + wiremockHttpPort + "/test-ca.crl");

    // Generate CRL with this certificate revoked
    byte[] crlContent = testCA.generateCRLWithRevokedCertificate(cert.getSerialNumber());
    setupCRLMapping("/test-ca.crl", crlContent, 200);

    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false)
            .build();
    validator = new CRLValidator(config);

    X509Certificate[] chain = {cert, testCA.getCACertificate()};
    List<X509Certificate[]> chains = Arrays.asList(new X509Certificate[][] {chain});

    CertificateException exception =
        assertThrows(CertificateException.class, () -> validator.validateCertificateChains(chains));

    assertTrue(exception.getMessage().contains("certificates are revoked"));
  }

  @Test
  void shouldPassInAdvisoryModeWithCRLErrors() throws Exception {
    // Setup WireMock to return 404 for CRL request
    setupCRLMapping("/test-ca.crl", "", 404);

    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ADVISORY)
            .allowCertificatesWithoutCrlUrl(false)
            .build();
    validator = new CRLValidator(config);

    X509Certificate cert =
        testCA.createCertificateWithCRLDistributionPoint(
            "CN=Test Server", "http://localhost:" + wiremockHttpPort + "/test-ca.crl");
    X509Certificate[] chain = {cert, testCA.getCACertificate()};

    List<X509Certificate[]> chains = Arrays.asList(new X509Certificate[][] {chain});
    boolean result = validator.validateCertificateChains(chains);

    assertTrue(result, "Should pass in advisory mode even with CRL fetch errors");
  }

  @Test
  void shouldFailInEnabledModeWithCRLErrors() throws Exception {
    // Setup WireMock to return 404 for CRL request
    setupCRLMapping("/test-ca.crl", "", 404);

    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false)
            .build();
    validator = new CRLValidator(config);

    X509Certificate cert =
        testCA.createCertificateWithCRLDistributionPoint(
            "CN=Test Server", "http://localhost:" + wiremockHttpPort + "/test-ca.crl");
    X509Certificate[] chain = {cert, testCA.getCACertificate()};

    List<X509Certificate[]> chains = Arrays.asList(new X509Certificate[][] {chain});

    assertThrows(CertificateException.class, () -> validator.validateCertificateChains(chains));
  }

  @Test
  void shouldValidateMultipleChainsAndReturnFirstValid() throws Exception {
    // Setup WireMock to serve valid CRL
    byte[] validCrlContent = testCA.generateValidCRL();
    setupCRLMapping("/valid-ca.crl", validCrlContent, 200);

    // Setup second CA that will fail
    setupCRLMapping("/invalid-ca.crl", "", 404);

    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false)
            .build();
    validator = new CRLValidator(config);

    // Create invalid chain first
    X509Certificate invalidCert =
        testCA.createCertificateWithCRLDistributionPoint(
            "CN=Invalid Server", "http://localhost:" + wiremockHttpPort + "/invalid-ca.crl");
    X509Certificate[] invalidChain = {invalidCert, testCA.getCACertificate()};

    // Create valid chain second
    X509Certificate validCert =
        testCA.createCertificateWithCRLDistributionPoint(
            "CN=Valid Server", "http://localhost:" + wiremockHttpPort + "/valid-ca.crl");
    X509Certificate[] validChain = {validCert, testCA.getCACertificate()};

    List<X509Certificate[]> chains = Arrays.asList(invalidChain, validChain);
    boolean result = validator.validateCertificateChains(chains);

    assertTrue(result, "Should find and validate the first valid chain");
  }

  @Test
  void shouldHandleExpiredCRL() throws Exception {
    // Generate expired CRL (nextUpdate in the past)
    byte[] expiredCrlContent = testCA.generateExpiredCRL();
    setupCRLMapping("/expired-ca.crl", expiredCrlContent, 200);

    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false)
            .build();
    validator = new CRLValidator(config);

    X509Certificate cert =
        testCA.createCertificateWithCRLDistributionPoint(
            "CN=Test Server", "http://localhost:" + wiremockHttpPort + "/expired-ca.crl");
    X509Certificate[] chain = {cert, testCA.getCACertificate()};

    List<X509Certificate[]> chains = Arrays.asList(new X509Certificate[][] {chain});

    assertThrows(CertificateException.class, () -> validator.validateCertificateChains(chains));
  }

  @Test
  void shouldSkipShortLivedCertificates() throws Exception {
    // Don't need to setup CRL mapping since short-lived certs should be skipped
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false)
            .build();
    validator = new CRLValidator(config);

    // Create short-lived certificate (5 days validity)
    X509Certificate shortLivedCert = testCA.createShortLivedCertificate(5);
    X509Certificate[] chain = {shortLivedCert, testCA.getCACertificate()};

    List<X509Certificate[]> chains = Arrays.asList(new X509Certificate[][] {chain});
    boolean result = validator.validateCertificateChains(chains);

    assertTrue(result, "Should skip validation for short-lived certificates");
  }

  @Test
  void shouldHandleMultipleCRLDistributionPoints() throws Exception {
    // Setup multiple CRL endpoints
    byte[] crlContent = testCA.generateValidCRL();
    setupCRLMapping("/primary-ca.crl", crlContent, 200);
    setupCRLMapping("/backup-ca.crl", crlContent, 200);

    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .certRevocationCheckMode(CRLValidationConfig.CertRevocationCheckMode.ENABLED)
            .allowCertificatesWithoutCrlUrl(false)
            .build();
    validator = new CRLValidator(config);

    // Create certificate with multiple CRL distribution points
    List<String> crlUrls =
        Arrays.asList(
            "http://localhost:" + wiremockHttpPort + "/primary-ca.crl",
            "http://localhost:" + wiremockHttpPort + "/backup-ca.crl");
    X509Certificate cert =
        testCA.createCertificateWithMultipleCRLDistributionPoints("CN=Multi-CRL Server", crlUrls);
    X509Certificate[] chain = {cert, testCA.getCACertificate()};

    List<X509Certificate[]> chains = Arrays.asList(new X509Certificate[][] {chain});
    boolean result = validator.validateCertificateChains(chains);

    assertTrue(result, "Should validate certificate with multiple CRL distribution points");
  }

  private void setupCRLMapping(String urlPath, byte[] crlContent, int statusCode)
      throws IOException {
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

  private void setupCRLMapping(String urlPath, String emptyContent, int statusCode)
      throws IOException {
    setupCRLMapping(urlPath, new byte[0], statusCode);
  }

  /** Test Certificate Authority for generating test certificates and CRLs */
  private static class TestCertificateAuthority {
    private final SecureRandom random = new SecureRandom();
    private final KeyPair caKeyPair;
    private final X509Certificate caCertificate;
    private final List<BigInteger> revokedSerialNumbers = new ArrayList<>();

    public TestCertificateAuthority() throws Exception {
      this.caKeyPair = generateKeyPair();
      this.caCertificate = createCACertificate();
    }

    public X509Certificate getCACertificate() {
      return caCertificate;
    }

    public X509Certificate createCertificateWithCRLDistributionPoint(
        String subjectDN, String crlUrl) throws Exception {
      return createCertificateWithMultipleCRLDistributionPoints(subjectDN, Arrays.asList(crlUrl));
    }

    public X509Certificate createCertificateWithMultipleCRLDistributionPoints(
        String subjectDN, List<String> crlUrls) throws Exception {
      KeyPair keyPair = generateKeyPair();

      long now = System.currentTimeMillis();
      Date notBefore = new Date(now);
      Date notAfter = new Date(now + 365L * 24 * 60 * 60 * 1000); // 1 year

      X509v3CertificateBuilder certBuilder =
          new JcaX509v3CertificateBuilder(
              new X500Name(caCertificate.getSubjectX500Principal().getName()),
              BigInteger.valueOf(random.nextLong()),
              notBefore,
              notAfter,
              new X500Name(subjectDN),
              keyPair.getPublic());

      certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
      certBuilder.addExtension(
          Extension.keyUsage,
          true,
          new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));

      // Add CRL Distribution Points extension
      GeneralName[] generalNames = new GeneralName[crlUrls.size()];
      for (int i = 0; i < crlUrls.size(); i++) {
        generalNames[i] = new GeneralName(GeneralName.uniformResourceIdentifier, crlUrls.get(i));
      }

      DistributionPointName dpName =
          new DistributionPointName(
              DistributionPointName.FULL_NAME, new GeneralNames(generalNames));

      DistributionPoint[] distributionPoints = {new DistributionPoint(dpName, null, null)};

      CRLDistPoint crlDistPoint = new CRLDistPoint(distributionPoints);
      certBuilder.addExtension(Extension.cRLDistributionPoints, true, crlDistPoint);

      ContentSigner contentSigner =
          new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(caKeyPair.getPrivate());
      X509CertificateHolder certHolder = certBuilder.build(contentSigner);

      return new JcaX509CertificateConverter()
          .setProvider(BOUNCY_CASTLE_PROVIDER)
          .getCertificate(certHolder);
    }

    public X509Certificate createShortLivedCertificate(int validityDays) throws Exception {
      KeyPair keyPair = generateKeyPair();

      long now = System.currentTimeMillis();
      Date notBefore = new Date(now);
      Date notAfter = new Date(now + validityDays * 24L * 60 * 60 * 1000);

      X509v3CertificateBuilder certBuilder =
          new JcaX509v3CertificateBuilder(
              new X500Name(caCertificate.getSubjectX500Principal().getName()),
              BigInteger.valueOf(random.nextLong()),
              notBefore,
              notAfter,
              new X500Name("CN=Short-Lived " + random.nextInt(10000)),
              keyPair.getPublic());

      certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
      certBuilder.addExtension(
          Extension.keyUsage,
          true,
          new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));

      ContentSigner contentSigner =
          new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(caKeyPair.getPrivate());
      X509CertificateHolder certHolder = certBuilder.build(contentSigner);

      return new JcaX509CertificateConverter()
          .setProvider(BOUNCY_CASTLE_PROVIDER)
          .getCertificate(certHolder);
    }

    public byte[] generateValidCRL() throws Exception {
      Date now = new Date();
      Date nextUpdate = new Date(now.getTime() + 24 * 60 * 60 * 1000); // 24 hours from now

      X509v2CRLBuilder crlBuilder =
          new X509v2CRLBuilder(
              new X500Name(caCertificate.getSubjectX500Principal().getName()), now);
      crlBuilder.setNextUpdate(nextUpdate);

      // Add revoked certificates
      for (BigInteger serialNumber : revokedSerialNumbers) {
        crlBuilder.addCRLEntry(serialNumber, now, 0); // 0 = unspecified reason
      }

      ContentSigner contentSigner =
          new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(caKeyPair.getPrivate());
      X509CRLHolder crlHolder = crlBuilder.build(contentSigner);

      X509CRL crl = new JcaX509CRLConverter().setProvider(BOUNCY_CASTLE_PROVIDER).getCRL(crlHolder);

      // Return CRL in DER format
      return crl.getEncoded();
    }

    public byte[] generateCRLWithRevokedCertificate(BigInteger serialNumber) throws Exception {
      revokedSerialNumbers.add(serialNumber);
      return generateValidCRL();
    }

    public byte[] generateExpiredCRL() throws Exception {
      Date now = new Date();
      Date pastDate = new Date(now.getTime() - 24 * 60 * 60 * 1000); // 24 hours ago

      X509v2CRLBuilder crlBuilder =
          new X509v2CRLBuilder(
              new X500Name(caCertificate.getSubjectX500Principal().getName()), pastDate);
      crlBuilder.setNextUpdate(pastDate); // Expired

      ContentSigner contentSigner =
          new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(caKeyPair.getPrivate());
      X509CRLHolder crlHolder = crlBuilder.build(contentSigner);

      X509CRL crl = new JcaX509CRLConverter().setProvider(BOUNCY_CASTLE_PROVIDER).getCRL(crlHolder);

      return crl.getEncoded();
    }

    private X509Certificate createCACertificate() throws Exception {
      long now = System.currentTimeMillis();
      Date notBefore = new Date(now);
      Date notAfter = new Date(now + 10L * 365 * 24 * 60 * 60 * 1000); // 10 years

      X509v3CertificateBuilder certBuilder =
          new JcaX509v3CertificateBuilder(
              new X500Name("CN=Test CA " + random.nextInt(10000)),
              BigInteger.valueOf(random.nextLong()),
              notBefore,
              notAfter,
              new X500Name("CN=Test CA " + random.nextInt(10000)),
              caKeyPair.getPublic());

      certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
      certBuilder.addExtension(
          Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));

      ContentSigner contentSigner =
          new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(caKeyPair.getPrivate());
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
  }
}
