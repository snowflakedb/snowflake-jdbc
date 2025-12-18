package net.snowflake.client.core.crl;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.CRLDistPoint;
import org.bouncycastle.asn1.x509.DistributionPoint;
import org.bouncycastle.asn1.x509.DistributionPointName;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.IssuingDistributionPoint;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CRLConverter;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v2CRLBuilder;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

public class CertificateGeneratorUtil {
  private static final String SIGNATURE_ALGORITHM = "SHA256WithRSA";
  private static final String BOUNCY_CASTLE_PROVIDER = "BC";
  private static final long ONE_YEAR_MS = 365L * 24 * 60 * 60 * 1000;
  private static final long ONE_DAY_MS = 24L * 60 * 60 * 1000;

  private final SecureRandom random = new SecureRandom();
  private KeyPair caKeyPair;
  private X509Certificate caCertificate;
  private final List<BigInteger> revokedSerialNumbers = new ArrayList<>();

  public CertificateGeneratorUtil() {
    try {
      Security.addProvider(new BouncyCastleProvider());
      this.caKeyPair = generateKeyPair();
      this.caCertificate = createCACertificate();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  X509Certificate getCACertificate() {
    return caCertificate;
  }

  private X509Certificate createCertificate(
      String subjectDN, boolean isCA, List<String> crlUrls, Date notBefore, Date notAfter)
      throws Exception {
    KeyPair keyPair = generateKeyPair();

    X509v3CertificateBuilder certBuilder =
        createBasicCertBuilder(
            keyPair.getPublic(),
            subjectDN,
            caCertificate.getSubjectX500Principal().getName(),
            isCA,
            notBefore,
            notAfter);

    if (crlUrls != null && !crlUrls.isEmpty()) {
      GeneralName[] generalNames =
          crlUrls.stream()
              .map(url -> new GeneralName(GeneralName.uniformResourceIdentifier, url))
              .toArray(GeneralName[]::new);

      DistributionPointName dpName =
          new DistributionPointName(
              DistributionPointName.FULL_NAME, new GeneralNames(generalNames));

      CRLDistPoint crlDistPoint =
          new CRLDistPoint(new DistributionPoint[] {new DistributionPoint(dpName, null, null)});
      certBuilder.addExtension(Extension.cRLDistributionPoints, true, crlDistPoint);
    }

    return buildCertificate(certBuilder, keyPair.getPrivate());
  }

  CertificateChain createSimpleChain() throws Exception {
    X509Certificate rootCert =
        createCertificate("CN=Test Root CA " + random.nextInt(10000), true, null, null, null);

    X509Certificate intermediateCert =
        createCertificate(
            "CN=Test Intermediate CA " + random.nextInt(10000), true, null, null, null);

    X509Certificate leafCert =
        createCertificate("CN=Test Leaf " + random.nextInt(10000), false, null, null, null);

    return new CertificateChain(rootCert, intermediateCert, leafCert);
  }

  X509Certificate createShortLivedCertificate(int validityDays, Date issuanceDate)
      throws Exception {
    Date notAfter = new Date(issuanceDate.getTime() + validityDays * ONE_DAY_MS);
    return createCertificate(
        "CN=Test Short-Lived " + random.nextInt(10000), false, null, issuanceDate, notAfter);
  }

  X509Certificate createCertificateWithCRLDistributionPoints(String subjectDN, List<String> crlUrls)
      throws Exception {
    return createCertificate(subjectDN, false, crlUrls, null, null);
  }

  public X509Certificate createWithIssuer(String issuerDN) throws Exception {
    KeyPair keyPair = generateKeyPair();
    X509v3CertificateBuilder certBuilder =
        createBasicCertBuilder(
            keyPair.getPublic(),
            "CN=Test Leaf " + random.nextInt(10000),
            issuerDN,
            false,
            null,
            null);
    return buildCertificate(certBuilder, caKeyPair.getPrivate());
  }

  X509CRL createCRLWithIDPDistributionPoints(
      X509Certificate issuerCert, List<String> distributionPointUrls) throws Exception {
    GeneralName[] generalNames =
        distributionPointUrls.stream()
            .map(url -> new GeneralName(GeneralName.uniformResourceIdentifier, url))
            .toArray(GeneralName[]::new);

    DistributionPointName dpName =
        new DistributionPointName(DistributionPointName.FULL_NAME, new GeneralNames(generalNames));

    IssuingDistributionPoint idp =
        new IssuingDistributionPoint(dpName, false, false, null, false, false);
    return createCRLWithIDP(issuerCert, idp);
  }

  byte[] generateValidCRL() throws Exception {
    return generateCRL(new Date(System.currentTimeMillis() + 24 * 60 * 60 * 1000)).getEncoded();
  }

  byte[] generateCRLWithRevokedCertificate(BigInteger serialNumber) throws Exception {
    revokedSerialNumbers.add(serialNumber);
    return generateValidCRL();
  }

  byte[] generateExpiredCRL() throws Exception {
    return generateCRL(new Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000)).getEncoded();
  }

  X509CRL generateCRL(Date nextUpdate) throws Exception {
    Date now = new Date();
    X509v2CRLBuilder crlBuilder =
        new X509v2CRLBuilder(new X500Name(caCertificate.getSubjectX500Principal().getName()), now);
    crlBuilder.setNextUpdate(nextUpdate);

    for (BigInteger serialNumber : revokedSerialNumbers) {
      crlBuilder.addCRLEntry(serialNumber, now, 0);
    }

    ContentSigner contentSigner =
        new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(caKeyPair.getPrivate());
    X509CRLHolder crlHolder = crlBuilder.build(contentSigner);
    return new JcaX509CRLConverter().setProvider(BOUNCY_CASTLE_PROVIDER).getCRL(crlHolder);
  }

  private KeyPair generateKeyPair() throws Exception {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    return keyGen.generateKeyPair();
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

  private X509v3CertificateBuilder createBasicCertBuilder(
      PublicKey publicKey,
      String subjectDN,
      String issuerDN,
      boolean isCA,
      Date notBefore,
      Date notAfter)
      throws CertIOException {

    if (notBefore == null) {
      notBefore = new Date();
    }
    if (notAfter == null) {
      notAfter = new Date(notBefore.getTime() + ONE_YEAR_MS);
    }

    X509v3CertificateBuilder certBuilder =
        new JcaX509v3CertificateBuilder(
            new X500Name(issuerDN),
            BigInteger.valueOf(random.nextLong()),
            notBefore,
            notAfter,
            new X500Name(subjectDN),
            publicKey);

    certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCA));

    KeyUsage keyUsage =
        isCA
            ? new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign)
            : new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment);
    certBuilder.addExtension(Extension.keyUsage, true, keyUsage);

    return certBuilder;
  }

  private X509Certificate buildCertificate(
      X509v3CertificateBuilder certBuilder, PrivateKey signerPrivateKey) throws Exception {
    ContentSigner contentSigner =
        new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(signerPrivateKey);
    X509CertificateHolder certHolder = certBuilder.build(contentSigner);
    return new JcaX509CertificateConverter()
        .setProvider(BOUNCY_CASTLE_PROVIDER)
        .getCertificate(certHolder);
  }

  X509CRL createCRLWithIDP(X509Certificate issuerCert, IssuingDistributionPoint idp)
      throws Exception {
    KeyPair issuerKeyPair = generateKeyPair();
    X509v2CRLBuilder crlBuilder =
        new JcaX509v2CRLBuilder(issuerCert.getSubjectX500Principal(), new Date());
    crlBuilder.setNextUpdate(new Date(System.currentTimeMillis() + ONE_DAY_MS));

    if (idp != null) {
      crlBuilder.addExtension(Extension.issuingDistributionPoint, true, idp);
    }

    return buildCRL(crlBuilder, issuerKeyPair.getPrivate());
  }

  private X509CRL buildCRL(X509v2CRLBuilder crlBuilder, PrivateKey signerPrivateKey)
      throws Exception {
    ContentSigner contentSigner =
        new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(signerPrivateKey);
    X509CRLHolder crlHolder = crlBuilder.build(contentSigner);
    return new JcaX509CRLConverter().setProvider(BOUNCY_CASTLE_PROVIDER).getCRL(crlHolder);
  }

  static class CertificateChain {
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
