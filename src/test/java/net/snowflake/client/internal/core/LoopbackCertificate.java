package net.snowflake.client.internal.core;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Generates a short-lived self-signed certificate for {@code localhost} / {@code 127.0.0.1}.
 *
 * <p>Needed because clients performing endpoint identification (the AWS SDK's Netty client does,
 * and a trust-all {@code TrustManager} does not bypass it) reject a certificate without a matching
 * subject alternative name. The committed {@code ssl-tests} certificate is issued for {@code
 * *.exampledata.com}, so it cannot serve a loopback endpoint.
 *
 * <p>Generated per run rather than committed, so there is nothing to expire or regenerate.
 */
final class LoopbackCertificate {

  static final String ALIAS = "loopback";
  static final char[] PASSWORD = "loopback".toCharArray();

  private LoopbackCertificate() {}

  /**
   * @return a PKCS12 keystore holding one private key entry usable by a loopback TLS server
   */
  static KeyStore newKeyStore() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    KeyPair keyPair = generator.generateKeyPair();

    long now = System.currentTimeMillis();
    X500Name subject = new X500Name("CN=localhost");
    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            subject,
            BigInteger.valueOf(now),
            new Date(now - 60_000L),
            new Date(now + 86_400_000L),
            subject,
            keyPair.getPublic());
    builder.addExtension(
        Extension.subjectAlternativeName,
        false,
        new GeneralNames(
            new GeneralName[] {
              new GeneralName(GeneralName.dNSName, "localhost"),
              new GeneralName(GeneralName.iPAddress, "127.0.0.1")
            }));

    ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate());
    X509Certificate certificate =
        new JcaX509CertificateConverter().getCertificate(builder.build(signer));

    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    keyStore.setKeyEntry(ALIAS, keyPair.getPrivate(), PASSWORD, new Certificate[] {certificate});
    return keyStore;
  }
}
