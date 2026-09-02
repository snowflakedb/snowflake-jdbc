package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Map;
import net.snowflake.client.internal.jdbc.OCSPErrorCode;
import net.snowflake.client.internal.util.SFPair;
import org.apache.commons.codec.binary.Base64;
import org.bouncycastle.asn1.ocsp.CertID;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.cert.ocsp.BasicOCSPResp;
import org.bouncycastle.cert.ocsp.BasicOCSPRespBuilder;
import org.bouncycastle.cert.ocsp.CertificateID;
import org.bouncycastle.cert.ocsp.CertificateStatus;
import org.bouncycastle.cert.ocsp.OCSPResp;
import org.bouncycastle.cert.ocsp.OCSPRespBuilder;
import org.bouncycastle.cert.ocsp.jcajce.JcaRespID;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DigestCalculatorProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.operator.jcajce.JcaDigestCalculatorProviderBuilder;
import org.junit.jupiter.api.Test;

/**
 * Robustness checks for OCSP response handling: a definitive verdict is treated as authoritative
 * even in fail-open mode, and each response entry is confirmed to correspond to the certificate
 * under validation (SNOW-3649698).
 */
public class SFTrustManagerOcspResponseValidationTest {

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
  private static final long ONE_YEAR_MS = 365L * 24 * 60 * 60 * 1000;
  private final SecureRandom random = new SecureRandom();

  /**
   * A definitive OCSP verdict must be authoritative regardless of OCSP mode; only the inability to
   * obtain a usable response is tolerable under fail-open.
   */
  @Test
  public void testFailOpenTreatsDefinitiveResultsAsAuthoritative() {
    // Definitive verdicts about the response or the certificate must always propagate.
    assertTrue(
        SFTrustManager.isDefinitiveRevocationResult(OCSPErrorCode.CERTIFICATE_STATUS_REVOKED),
        "A revoked verdict must be authoritative");
    assertTrue(
        SFTrustManager.isDefinitiveRevocationResult(OCSPErrorCode.CERTIFICATE_ID_MISMATCH),
        "A CertID mismatch must be authoritative");
    assertTrue(
        SFTrustManager.isDefinitiveRevocationResult(OCSPErrorCode.INVALID_OCSP_RESPONSE_SIGNATURE),
        "An untrusted response signature must be authoritative");
    assertTrue(
        SFTrustManager.isDefinitiveRevocationResult(OCSPErrorCode.INVALID_CERTIFICATE_SIGNATURE),
        "An untrusted signing certificate must be authoritative");

    // Genuinely soft cases remain tolerable, including RFC unknown: that branch is only
    // reachable after signature verification and CertID binding, so it is a responder
    // statement rather than a substitution (matching Python and Node).
    assertFalse(
        SFTrustManager.isDefinitiveRevocationResult(OCSPErrorCode.CERTIFICATE_STATUS_UNKNOWN),
        "An unknown status must remain tolerable under fail-open");
    assertFalse(
        SFTrustManager.isDefinitiveRevocationResult(OCSPErrorCode.OCSP_RESPONSE_FETCH_FAILURE),
        "A fetch failure must remain tolerable under fail-open");
    assertFalse(
        SFTrustManager.isDefinitiveRevocationResult(OCSPErrorCode.INVALID_OCSP_RESPONSE_VALIDITY),
        "A validity-window issue must remain tolerable under fail-open");
    assertFalse(
        SFTrustManager.isDefinitiveRevocationResult(OCSPErrorCode.OCSP_RESPONSE_FETCH_TIMEOUT),
        "A fetch timeout must remain tolerable under fail-open");
  }

  /**
   * A response entry that describes a different certificate than the one being validated must not
   * be accepted, even when it reports a GOOD status.
   */
  @Test
  public void testOcspResponseMustMatchRequestedCertificate() throws Exception {
    KeyPair issuerKeyPair = generateKeyPair();
    X509Certificate issuerCert =
        createSelfSignedCa(issuerKeyPair, "CN=Test Issuer " + random.nextInt(100000));

    // The certificate actually being validated.
    X509Certificate subjectCert =
        createLeaf(issuerCert, issuerKeyPair, "CN=Test Subject " + random.nextInt(100000));
    // A different certificate whose CertID will be placed in the response instead.
    X509Certificate otherCert =
        createLeaf(issuerCert, issuerKeyPair, "CN=Test Other " + random.nextInt(100000));

    org.bouncycastle.asn1.x509.Certificate bcIssuer =
        org.bouncycastle.asn1.x509.Certificate.getInstance(issuerCert.getEncoded());
    org.bouncycastle.asn1.x509.Certificate bcSubject =
        org.bouncycastle.asn1.x509.Certificate.getInstance(subjectCert.getEncoded());

    // Build a well-formed, correctly-signed OCSP response that reports GOOD, but for the CertID of
    // a different certificate than the one under validation.
    String ocspRespB64 =
        buildSignedOcspResponse(issuerCert, issuerKeyPair, otherCert, CertificateStatus.GOOD);

    SFTrustManager tm = new SFTrustManager(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN), null);

    SFOCSPException ex =
        assertThrows(
            SFOCSPException.class,
            () -> tm.validateRevocationStatusMain(SFPair.of(bcIssuer, bcSubject), ocspRespB64),
            "A response describing a different certificate must not be accepted");

    assertSame(
        OCSPErrorCode.CERTIFICATE_ID_MISMATCH,
        ex.getErrorCode(),
        "A response that does not correspond to the requested certificate must be rejected"
            + " with a definitive (non-tolerable) result");
    assertTrue(
        SFTrustManager.isDefinitiveRevocationResult(ex.getErrorCode()),
        "The mismatch result must flow through the non-tolerable path");
  }

  /**
   * Control case: when the response entry corresponds to the certificate under validation, a GOOD
   * status is accepted without error.
   */
  @Test
  public void testOcspResponseForRequestedCertificateIsAccepted() throws Throwable {
    KeyPair issuerKeyPair = generateKeyPair();
    X509Certificate issuerCert =
        createSelfSignedCa(issuerKeyPair, "CN=Test Issuer " + random.nextInt(100000));
    X509Certificate subjectCert =
        createLeaf(issuerCert, issuerKeyPair, "CN=Test Subject " + random.nextInt(100000));

    org.bouncycastle.asn1.x509.Certificate bcIssuer =
        org.bouncycastle.asn1.x509.Certificate.getInstance(issuerCert.getEncoded());
    org.bouncycastle.asn1.x509.Certificate bcSubject =
        org.bouncycastle.asn1.x509.Certificate.getInstance(subjectCert.getEncoded());

    String ocspRespB64 =
        buildSignedOcspResponse(issuerCert, issuerKeyPair, subjectCert, CertificateStatus.GOOD);

    SFTrustManager tm = new SFTrustManager(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN), null);

    // Should complete without throwing when the CertID matches.
    tm.validateRevocationStatusMain(SFPair.of(bcIssuer, bcSubject), ocspRespB64);
  }

  /**
   * A cache entry written before validation must be removed when the validation produces a
   * definitive result, so that subsequent connections re-fetch rather than re-reading a stale entry
   * indefinitely (SNOW-3649698).
   */
  @Test
  public void testDefinitiveFailureEvictsCacheEntry() throws Exception {
    KeyPair issuerKeyPair = generateKeyPair();
    X509Certificate issuerCert =
        createSelfSignedCa(issuerKeyPair, "CN=Eviction Issuer " + random.nextInt(100000));
    X509Certificate subjectCert =
        createLeaf(issuerCert, issuerKeyPair, "CN=Eviction Subject " + random.nextInt(100000));
    X509Certificate otherCert =
        createLeaf(issuerCert, issuerKeyPair, "CN=Eviction Other " + random.nextInt(100000));

    // Derive the OcspResponseCacheKey that executeOneRevocationStatusCheck would produce for
    // subjectCert.  The driver's createRequest() uses SHA1DigestCalculator to build the OCSP
    // request, then extracts the DER-encoded hash bytes from the resulting CertID ASN1 primitive.
    // Replicate the same derivation so the cache lookup hits the entry we seed below.
    DigestCalculatorProvider digestProvider =
        new JcaDigestCalculatorProviderBuilder().setProvider("BC").build();
    X509CertificateHolder issuerHolder = new X509CertificateHolder(issuerCert.getEncoded());
    CertID cidAsn1 =
        CertID.getInstance(
            new CertificateID(
                    digestProvider.get(CertificateID.HASH_SHA1),
                    issuerHolder,
                    subjectCert.getSerialNumber())
                .toASN1Primitive());
    SFTrustManager.OcspResponseCacheKey key =
        new SFTrustManager.OcspResponseCacheKey(
            cidAsn1.getIssuerNameHash().getEncoded(),
            cidAsn1.getIssuerKeyHash().getEncoded(),
            cidAsn1.getSerialNumber().getValue());

    // Simulate the pre-validation cache write: a response that covers otherCert's CertID.
    String mismatchedRespB64 =
        buildSignedOcspResponse(issuerCert, issuerKeyPair, otherCert, CertificateStatus.GOOD);

    Field cacheField = SFTrustManager.class.getDeclaredField("OCSP_RESPONSE_CACHE");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<SFTrustManager.OcspResponseCacheKey, SFPair<Long, String>> cache =
        (Map<SFTrustManager.OcspResponseCacheKey, SFPair<Long, String>>) cacheField.get(null);

    cache.put(key, SFPair.of(System.currentTimeMillis() / 1000, mismatchedRespB64));
    try {
      assertTrue(cache.containsKey(key), "Pre-seeded cache entry must be present before the call");

      org.bouncycastle.asn1.x509.Certificate bcIssuer =
          org.bouncycastle.asn1.x509.Certificate.getInstance(issuerCert.getEncoded());
      org.bouncycastle.asn1.x509.Certificate bcSubject =
          org.bouncycastle.asn1.x509.Certificate.getInstance(subjectCert.getEncoded());

      SFTrustManager tm = new SFTrustManager(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN), null);
      Method method =
          SFTrustManager.class.getDeclaredMethod(
              "executeOneRevocationStatusCheck", SFPair.class, long.class, String.class);
      method.setAccessible(true);

      InvocationTargetException ex =
          assertThrows(
              InvocationTargetException.class,
              () ->
                  method.invoke(
                      tm,
                      SFPair.of(bcIssuer, bcSubject),
                      System.currentTimeMillis() / 1000,
                      "test.snowflakecomputing.com"));
      assertInstanceOf(
          java.security.cert.CertificateException.class,
          ex.getCause(),
          "A definitive failure must surface as CertificateException");

      assertFalse(
          cache.containsKey(key),
          "Cache entry must be evicted after a definitive failure so it is not retained"
              + " permanently (SNOW-3649698)");
    } finally {
      cache.remove(key);
    }
  }

  /**
   * Builds a base64-encoded, SUCCESSFUL OCSP response signed by the issuer key. The single response
   * entry carries the CertID of {@code certForCertId} with the supplied status. Uses a SHA-1 CertID
   * to mirror the request the driver builds.
   */
  private String buildSignedOcspResponse(
      X509Certificate issuerCert,
      KeyPair issuerKeyPair,
      X509Certificate certForCertId,
      CertificateStatus status)
      throws Exception {
    X509CertificateHolder issuerHolder = new X509CertificateHolder(issuerCert.getEncoded());
    DigestCalculatorProvider digestProvider =
        new JcaDigestCalculatorProviderBuilder().setProvider("BC").build();
    CertificateID certId =
        new CertificateID(
            digestProvider.get(CertificateID.HASH_SHA1),
            issuerHolder,
            certForCertId.getSerialNumber());

    BasicOCSPRespBuilder respBuilder =
        new BasicOCSPRespBuilder(new JcaRespID(issuerCert.getSubjectX500Principal()));
    Date thisUpdate = new Date(System.currentTimeMillis() - 60_000L);
    Date nextUpdate = new Date(System.currentTimeMillis() + 24L * 60 * 60 * 1000);
    respBuilder.addResponse(certId, status, thisUpdate, nextUpdate, null);

    ContentSigner signer =
        new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
            .setProvider("BC")
            .build(issuerKeyPair.getPrivate());
    BasicOCSPResp basicResp = respBuilder.build(signer, null, new Date());

    OCSPResp ocspResp = new OCSPRespBuilder().build(OCSPRespBuilder.SUCCESSFUL, basicResp);
    return Base64.encodeBase64String(ocspResp.getEncoded());
  }

  private KeyPair generateKeyPair() throws Exception {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    return keyGen.generateKeyPair();
  }

  private X509Certificate createSelfSignedCa(KeyPair keyPair, String dn) throws Exception {
    Date notBefore = new Date(System.currentTimeMillis() - ONE_YEAR_MS);
    Date notAfter = new Date(System.currentTimeMillis() + ONE_YEAR_MS);
    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            new X500Name(dn),
            BigInteger.valueOf(Math.abs(random.nextLong())),
            notBefore,
            notAfter,
            new X500Name(dn),
            keyPair.getPublic());
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
    builder.addExtension(
        Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
    return sign(builder, keyPair.getPrivate());
  }

  private X509Certificate createLeaf(
      X509Certificate issuerCert, KeyPair issuerKeyPair, String subjectDn) throws Exception {
    KeyPair leafKeyPair = generateKeyPair();
    return createLeafWithKey(issuerCert, issuerKeyPair, subjectDn, leafKeyPair.getPublic());
  }

  private X509Certificate createLeafWithKey(
      X509Certificate issuerCert, KeyPair issuerKeyPair, String subjectDn, PublicKey subjectKey)
      throws Exception {
    Date notBefore = new Date(System.currentTimeMillis() - ONE_YEAR_MS);
    Date notAfter = new Date(System.currentTimeMillis() + ONE_YEAR_MS);
    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            new X500Name(issuerCert.getSubjectX500Principal().getName()),
            BigInteger.valueOf(Math.abs(random.nextLong())),
            notBefore,
            notAfter,
            new X500Name(subjectDn),
            subjectKey);
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
    return sign(builder, issuerKeyPair.getPrivate());
  }

  private X509Certificate sign(JcaX509v3CertificateBuilder builder, PrivateKey signerKey)
      throws Exception {
    ContentSigner signer =
        new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).setProvider("BC").build(signerKey);
    X509CertificateHolder holder = builder.build(signer);
    return new JcaX509CertificateConverter().setProvider("BC").getCertificate(holder);
  }
}
