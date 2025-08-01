package net.snowflake.client.core.crl;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.X509CRL;
import java.security.cert.X509CRLEntry;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.bouncycastle.asn1.ASN1OctetString;
import org.bouncycastle.asn1.ASN1Primitive;
import org.bouncycastle.asn1.DERIA5String;
import org.bouncycastle.asn1.x509.CRLDistPoint;
import org.bouncycastle.asn1.x509.DistributionPoint;
import org.bouncycastle.asn1.x509.DistributionPointName;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.IssuingDistributionPoint;

/** CRL (Certificate Revocation List) validator implementation. */
public class CRLValidator {

  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLValidator.class);

  // CA/Browser Forum Baseline Requirements date thresholds (using UTC for consistency)
  private static final Date MARCH_15_2024 =
      Date.from(LocalDate.of(2024, 3, 15).atStartOfDay(ZoneId.of("UTC")).toInstant());
  private static final Date MARCH_15_2026 =
      Date.from(LocalDate.of(2026, 3, 15).atStartOfDay(ZoneId.of("UTC")).toInstant());

  private final CRLValidationConfig config;
  private final ConcurrentHashMap<String, ReentrantLock> urlLocks = new ConcurrentHashMap<>();

  public CRLValidator(CRLValidationConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("CRL validation config cannot be null");
    }
    this.config = config;
  }

  /**
   * Validates certificate chains against CRLs.
   *
   * @param certificateChains the verified certificate chains to validate
   * @return true if validation passes, false otherwise
   * @throws CertificateException if validation fails in ENABLED mode or certificate is revoked
   */
  public boolean validateCertificateChains(List<X509Certificate[]> certificateChains)
      throws CertificateException {
    if (config.getCertRevocationCheckMode()
        == CRLValidationConfig.CertRevocationCheckMode.DISABLED) {
      logger.debug("CRL validation is disabled");
      return true; // OPEN
    }

    if (certificateChains == null || certificateChains.isEmpty()) {
      throw new IllegalArgumentException("Certificate chains cannot be null or empty");
    }

    logger.debug("Validating {} certificate chains", certificateChains.size());

    List<CRLValidationResult> crlValidationResults = validateChains(certificateChains);

    if (crlValidationResults.contains(CRLValidationResult.CHAIN_UNREVOKED)) {
      logger.debug("Found certificate chain with all certificates unrevoked");
      return true; // OPEN
    }

    if (containsOnlyRevokedChains(crlValidationResults)) {
      logger.error("Every verified certificate chain contained revoked certificates");
      throw new CertificateException(
          "Certificate chain validation failed: certificates are revoked");
    }

    logger.warn("Some certificate chains didn't pass or driver wasn't able to perform the checks");
    if (config.getCertRevocationCheckMode()
        == CRLValidationConfig.CertRevocationCheckMode.ADVISORY) {
      logger.debug("Advisory mode: allowing connection despite validation issues");
      return true; // FAIL OPEN
    }

    throw new CertificateException("Certificate chain validation failed");
  }

  /** Validates multiple certificate chains until finding a valid chain with no revocations. */
  private List<CRLValidationResult> validateChains(List<X509Certificate[]> certChains) {
    List<CRLValidationResult> chainsValidationResults = new ArrayList<>();

    for (X509Certificate[] certChain : certChains) {
      CRLValidationResult chainResult = CRLValidationResult.CHAIN_UNREVOKED;

      // For every cert in a chain except root (trust anchor)
      for (int i = 0; i < certChain.length - 1; i++) {
        X509Certificate cert = certChain[i];
        X509Certificate parentCert = certChain[i + 1]; // Next certificate in chain is the parent

        if (isShortLived(cert)) {
          logger.debug("Skipping short-lived certificate: {}", cert.getSubjectX500Principal());
          continue;
        }

        List<String> crlUrls = extractCRLDistributionPoints(cert);
        if (crlUrls.isEmpty()) {
          if (config.isAllowCertificatesWithoutCrlUrl()) {
            logger.warn(
                "Certificate has missing CRL Distribution Point URLs: {}",
                cert.getSubjectX500Principal());
            continue;
          }
          chainResult = CRLValidationResult.CHAIN_ERROR;
          continue;
        }

        CertificateValidationResult certStatus = validateCert(cert, parentCert);

        if (certStatus == CertificateValidationResult.CERT_REVOKED) {
          chainResult = CRLValidationResult.CHAIN_REVOKED;
          break;
        }

        if (certStatus == CertificateValidationResult.CERT_ERROR) {
          chainResult = CRLValidationResult.CHAIN_ERROR;
        }
      }

      chainsValidationResults.add(chainResult);

      if (chainResult == CRLValidationResult.CHAIN_UNREVOKED) {
        logger.debug("Found valid certificate chain, stopping validation of remaining chains");
        break;
      }
    }

    return chainsValidationResults;
  }

  /**
   * Checks if cert is not revoked on any of its CRL URLs.
   *
   * @param cert the certificate to validate
   * @param parentCert the parent certificate that should have issued the cert
   */
  private CertificateValidationResult validateCert(
      X509Certificate cert, X509Certificate parentCert) {
    List<String> crlUrls = extractCRLDistributionPoints(cert);

    for (String url : crlUrls) {
      CertificateValidationResult result = validateCert(cert, url, parentCert);

      if (result == CertificateValidationResult.CERT_REVOKED
          || result == CertificateValidationResult.CERT_ERROR) {
        return result;
      }
    }

    return CertificateValidationResult.CERT_UNREVOKED;
  }

  /**
   * Validates certificate against a specific CRL URL. Implements caching logic and CRL fetching.
   *
   * @param cert the certificate to validate
   * @param crlUrl the CRL URL to fetch from
   * @param parentCert the parent certificate that should have issued the cert
   */
  private CertificateValidationResult validateCert(
      X509Certificate cert, String crlUrl, X509Certificate parentCert) {
    // Thread-safe processing of CRL for given crlUrl
    ReentrantLock lock = urlLocks.computeIfAbsent(crlUrl, k -> new ReentrantLock());

    lock.lock();
    try {
      // TODO: Implement caching mechanism (getFromCache, updateCache)
      // For now, always fetch fresh CRL
      X509CRL crl = fetchCrl(crlUrl);

      if (crl == null) {
        logger.error("Unable to fetch CRL from {}", crlUrl);
        return CertificateValidationResult.CERT_ERROR;
      }

      if (!isCrlSignatureAndIssuerValid(crl, cert, parentCert)) {
        logger.error("Unable to verify CRL for {}", crlUrl);
        return CertificateValidationResult.CERT_ERROR;
      }

      // TODO: Update cache with new CRL

      if (isCertificateRevoked(crl, cert)) {
        logger.warn(
            "Certificate {} is revoked according to CRL {}", cert.getSerialNumber(), crlUrl);
        return CertificateValidationResult.CERT_REVOKED;
      }

      return CertificateValidationResult.CERT_UNREVOKED;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Validates CRL signature and issuer.
   *
   * @param crl the CRL to validate
   * @param cert the certificate being validated
   * @param parentCert the parent certificate that should have issued the cert and signed the CRL
   */
  private boolean isCrlSignatureAndIssuerValid(
      X509CRL crl, X509Certificate cert, X509Certificate parentCert) {
    try {
      // Verify CRL signature and issuer match
      if (!crl.getIssuerX500Principal().equals(parentCert.getSubjectX500Principal())) {
        logger.debug(
            "CRL issuer {} does not match parent certificate subject {} for {}",
            crl.getIssuerX500Principal(),
            parentCert.getSubjectX500Principal(),
            "validation");
        return false;
      }

      // Check CRL validity period first (fail fast)
      Date now = new Date();
      if (crl.getNextUpdate() != null && now.after(crl.getNextUpdate())) {
        logger.debug("CRL has expired: nextUpdate={}, now={}", crl.getNextUpdate(), now);
        return false;
      }

      if (crl.getThisUpdate() != null && now.before(crl.getThisUpdate())) {
        logger.debug("CRL is not yet valid: thisUpdate={}, now={}", crl.getThisUpdate(), now);
        return false;
      }

      // Verify CRL signature using parent certificate's public key
      PublicKey parentPublicKey = parentCert.getPublicKey();
      try {
        crl.verify(parentPublicKey);
        logger.debug("CRL signature verified successfully using parent certificate");
      } catch (InvalidKeyException
          | NoSuchAlgorithmException
          | NoSuchProviderException
          | SignatureException e) {
        logger.debug("CRL signature verification failed: {}", e.getMessage());
        return false;
      }

      // Verify IDP (Issuing Distribution Point) extension if present
      if (!verifyIssuingDistributionPoint(crl, cert)) {
        logger.debug("IDP extension verification failed");
        return false;
      }

      return true;
    } catch (Exception e) {
      logger.debug("CRL validation failed: {}", e.getMessage());
      return false;
    }
  }

  /** Checks if certificate is revoked according to CRL. */
  private boolean isCertificateRevoked(X509CRL crl, X509Certificate cert) {
    X509CRLEntry entry = crl.getRevokedCertificate(cert.getSerialNumber());
    return entry != null;
  }

  /** Fetches CRL from URL. */
  private X509CRL fetchCrl(String crlUrl) {
    try {
      logger.debug("Fetching CRL from {}", crlUrl);

      URL url = new URL(crlUrl);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setConnectTimeout(config.getConnectionTimeoutMs());
      connection.setReadTimeout(config.getReadTimeoutMs());
      connection.setRequestMethod("GET");

      try (InputStream inputStream = connection.getInputStream()) {
        java.security.cert.CertificateFactory cf =
            java.security.cert.CertificateFactory.getInstance("X.509");
        return (X509CRL) cf.generateCRL(inputStream);
      }
    } catch (IOException | CRLException | java.security.cert.CertificateException e) {
      logger.debug("Failed to fetch CRL from {}: {}", crlUrl, e.getMessage());
      return null;
    }
  }

  /**
   * Extracts CRL Distribution Points from certificate. Parses the CRL Distribution Points extension
   * (OID 2.5.29.31) and extracts HTTP URLs.
   */
  private List<String> extractCRLDistributionPoints(X509Certificate cert) {
    List<String> crlUrls = new ArrayList<>();

    try {
      // Get CRL Distribution Points extension (OID 2.5.29.31)
      byte[] extensionBytes = cert.getExtensionValue("2.5.29.31");
      if (extensionBytes == null) {
        logger.debug(
            "No CRL Distribution Points extension found for certificate: {}",
            cert.getSubjectX500Principal());
        return crlUrls;
      }

      // Parse the extension value (it's wrapped in an OCTET STRING)
      ASN1OctetString octetString = (ASN1OctetString) ASN1Primitive.fromByteArray(extensionBytes);
      CRLDistPoint distPoint =
          CRLDistPoint.getInstance(ASN1Primitive.fromByteArray(octetString.getOctets()));

      if (distPoint != null) {
        DistributionPoint[] distributionPoints = distPoint.getDistributionPoints();

        for (DistributionPoint dp : distributionPoints) {
          DistributionPointName dpn = dp.getDistributionPoint();
          if (dpn != null && dpn.getType() == DistributionPointName.FULL_NAME) {
            GeneralNames generalNames = (GeneralNames) dpn.getName();

            for (GeneralName generalName : generalNames.getNames()) {
              if (generalName.getTagNo() == GeneralName.uniformResourceIdentifier) {
                String url = ((DERIA5String) generalName.getName()).getString();
                if (url.toLowerCase().startsWith("http://")
                    || url.toLowerCase().startsWith("https://")) {
                  logger.debug("Found CRL URL: {}", url);
                  crlUrls.add(url);
                }
              }
            }
          }
        }
      }

    } catch (Exception e) {
      logger.debug(
          "Failed to extract CRL distribution points from certificate {}: {}",
          cert.getSubjectX500Principal(),
          e.getMessage());
    }

    logger.debug(
        "Extracted {} CRL URLs for certificate: {}",
        crlUrls.size(),
        cert.getSubjectX500Principal());
    return crlUrls;
  }

  /**
   * Checks if certificate is short-lived (doesn't require CRL validation). Follows CA/Browser Forum
   * Baseline Requirements for short-lived certificates.
   *
   * @see <a
   *     href="https://cabforum.org/working-groups/server/baseline-requirements/requirements/">CA/Browser
   *     Forum Baseline Requirements</a>
   */
  private boolean isShortLived(X509Certificate cert) {
    // Certificates issued before March 15, 2024 are not considered short-lived
    if (cert.getNotBefore().before(MARCH_15_2024)) {
      return false;
    }

    // Default maximum validity period is 7 days (for certificates issued after March 15, 2026)
    long maximumValidityPeriodMs = 7L * 24 * 60 * 60 * 1000;

    // For certificates issued between March 15, 2024 and March 15, 2026, maximum is 10 days
    if (cert.getNotBefore().before(MARCH_15_2026)) {
      maximumValidityPeriodMs = 10L * 24 * 60 * 60 * 1000;
    }

    // Add 1 minute margin to handle timing precision issues
    maximumValidityPeriodMs += 60 * 1000;

    long certValidityPeriodMs = cert.getNotAfter().getTime() - cert.getNotBefore().getTime();
    return maximumValidityPeriodMs > certValidityPeriodMs;
  }

  private boolean containsOnlyRevokedChains(List<CRLValidationResult> results) {
    return !results.isEmpty()
        && results.stream().allMatch(result -> result == CRLValidationResult.CHAIN_REVOKED);
  }

  /**
   * Verifies the Issuing Distribution Point (IDP) extension if present. The IDP extension restricts
   * which certificates this CRL covers.
   */
  private boolean verifyIssuingDistributionPoint(X509CRL crl, X509Certificate cert) {
    try {
      // Get IDP extension (OID 2.5.29.28)
      byte[] extensionBytes = crl.getExtensionValue("2.5.29.28");
      if (extensionBytes == null) {
        // No IDP extension means the CRL covers all certificates issued by this CA
        logger.debug("No IDP extension found - CRL covers all certificates");
        return true;
      }

      // Parse the IDP extension
      ASN1OctetString octetString = (ASN1OctetString) ASN1Primitive.fromByteArray(extensionBytes);
      IssuingDistributionPoint idp =
          IssuingDistributionPoint.getInstance(
              ASN1Primitive.fromByteArray(octetString.getOctets()));

      // Check if this CRL only covers user certificates
      if (idp.onlyContainsUserCerts() && cert.getBasicConstraints() != -1) {
        logger.debug("CRL only covers user certificates, but certificate is a CA certificate");
        return false;
      }

      // Check if this CRL only covers CA certificates
      if (idp.onlyContainsCACerts() && cert.getBasicConstraints() == -1) {
        logger.debug("CRL only covers CA certificates, but certificate is not a CA certificate");
        return false;
      }

      // Check distribution point name if present
      DistributionPointName dpName = idp.getDistributionPoint();
      if (dpName != null) {
        // The distribution point in the IDP should match one of the CRL distribution points
        // in the certificate being validated
        List<String> certCRLUrls = extractCRLDistributionPoints(cert);

        if (dpName.getType() == DistributionPointName.FULL_NAME) {
          GeneralNames generalNames = (GeneralNames) dpName.getName();
          boolean foundMatch = false;

          for (GeneralName generalName : generalNames.getNames()) {
            if (generalName.getTagNo() == GeneralName.uniformResourceIdentifier) {
              String idpUrl = ((DERIA5String) generalName.getName()).getString();
              if (certCRLUrls.contains(idpUrl)) {
                foundMatch = true;
                break;
              }
            }
          }

          if (!foundMatch) {
            logger.debug(
                "IDP distribution point does not match certificate's CRL distribution points");
            return false;
          }
        }
      }

      logger.debug("IDP extension verification passed");
      return true;
    } catch (Exception e) {
      logger.debug("Failed to verify IDP extension: {}", e.getMessage());
      return false;
    }
  }
}
