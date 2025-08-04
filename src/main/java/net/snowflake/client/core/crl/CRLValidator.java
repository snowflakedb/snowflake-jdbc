package net.snowflake.client.core.crl;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509CRL;
import java.security.cert.X509CRLEntry;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.bouncycastle.asn1.ASN1OctetString;
import org.bouncycastle.asn1.ASN1Primitive;
import org.bouncycastle.asn1.DERIA5String;
import org.bouncycastle.asn1.x509.CRLDistPoint;
import org.bouncycastle.asn1.x509.DistributionPoint;
import org.bouncycastle.asn1.x509.DistributionPointName;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.IssuingDistributionPoint;

class CRLValidator {

  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLValidator.class);

  // CA/Browser Forum Baseline Requirements date thresholds (using UTC for consistency)
  private static final Date MARCH_15_2024 =
      Date.from(LocalDate.of(2024, 3, 15).atStartOfDay(ZoneId.of("UTC")).toInstant());
  private static final Date MARCH_15_2026 =
      Date.from(LocalDate.of(2026, 3, 15).atStartOfDay(ZoneId.of("UTC")).toInstant());

  private final CRLValidationConfig config;
  private final CloseableHttpClient httpClient;
  private final Map<String, Lock> urlLocks = new ConcurrentHashMap<>();

  CRLValidator(CRLValidationConfig config, CloseableHttpClient httpClient) {
    this.httpClient = httpClient;
    this.config = config;
  }

  /**
   * Validates certificate chains against CRLs.
   *
   * @param certificateChains the verified certificate chains to validate
   * @return true if validation passes, false otherwise
   */
  boolean validateCertificateChains(List<X509Certificate[]> certificateChains) {
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

    if (crlValidationResults.get(crlValidationResults.size() - 1)
        == CRLValidationResult.CHAIN_UNREVOKED) {
      logger.debug("Found certificate chain with all certificates unrevoked");
      return true; // OPEN
    }

    if (containsOnlyRevokedChains(crlValidationResults)) {
      logger.debug("Every verified certificate chain contained revoked certificates");
      return false;
    }

    logger.debug("Some certificate chains didn't pass or driver wasn't able to perform the checks");
    if (config.getCertRevocationCheckMode()
        == CRLValidationConfig.CertRevocationCheckMode.ADVISORY) {
      logger.debug("Advisory mode: allowing connection despite validation issues");
      return true; // FAIL OPEN
    }

    return false;
  }

  private List<CRLValidationResult> validateChains(List<X509Certificate[]> certChains) {
    List<CRLValidationResult> chainsValidationResults = new ArrayList<>();

    for (X509Certificate[] certChain : certChains) {
      CRLValidationResult chainResult = CRLValidationResult.CHAIN_UNREVOKED;

      // Validate each certificate in the chain against CRL, skip the root certificate
      for (int i = 0; i < certChain.length; i++) {
        X509Certificate cert = certChain[i];
        boolean isRoot = (i == certChain.length - 1);
        // Don't validate root certificates against CRL - they are trust anchors
        if (isRoot) {
          break;
        }

        X509Certificate parentCert = certChain[i + 1];

        if (isShortLived(cert)) {
          logger.debug("Skipping short-lived certificate: {}", cert.getSubjectX500Principal());
          continue;
        }

        List<String> crlUrls = extractCRLDistributionPoints(cert);
        if (crlUrls.isEmpty()) {
          if (config.isAllowCertificatesWithoutCrlUrl()) {
            logger.debug(
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

  private CertificateValidationResult validateCert(
      X509Certificate cert, String crlUrl, X509Certificate parentCert) {
    // Thread-safe processing of CRL for given crlUrl
    Lock lock = urlLocks.computeIfAbsent(crlUrl, k -> new ReentrantLock());

    lock.lock();
    try {
      // TODO: Implement caching mechanism (getFromCache, updateCache)
      // For now, always fetch fresh CRL
      X509CRL crl = fetchCrl(crlUrl);

      if (crl == null) {
        return CertificateValidationResult.CERT_ERROR;
      }

      if (!isCrlSignatureAndIssuerValid(crl, cert, parentCert, crlUrl)) {
        logger.debug("Unable to verify CRL for {}", crlUrl);
        return CertificateValidationResult.CERT_ERROR;
      }

      // TODO: Update cache with new CRL

      if (isCertificateRevoked(crl, cert)) {
        logger.debug(
            "Certificate {} is revoked according to CRL {}", cert.getSerialNumber(), crlUrl);
        return CertificateValidationResult.CERT_REVOKED;
      }

      return CertificateValidationResult.CERT_UNREVOKED;
    } finally {
      lock.unlock();
    }
  }

  private boolean isCrlSignatureAndIssuerValid(
      X509CRL crl, X509Certificate cert, X509Certificate parentCert, String crlUrl) {
    try {
      if (!crl.getIssuerX500Principal().equals(parentCert.getSubjectX500Principal())) {
        logger.debug(
            "CRL issuer {} does not match parent certificate subject {} for {}",
            crl.getIssuerX500Principal(),
            parentCert.getSubjectX500Principal(),
            "validation");
        return false;
      }

      Date now = new Date();
      if (crl.getNextUpdate() != null && now.after(crl.getNextUpdate())) {
        logger.debug("CRL has expired: nextUpdate={}, now={}", crl.getNextUpdate(), now);
        return false;
      }

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

      if (!verifyIssuingDistributionPoint(crl, cert, crlUrl)) {
        logger.debug("IDP extension verification failed");
        return false;
      }

      return true;
    } catch (Exception e) {
      logger.debug("CRL validation failed: {}", e.getMessage());
      return false;
    }
  }

  private boolean isCertificateRevoked(X509CRL crl, X509Certificate cert) {
    X509CRLEntry entry = crl.getRevokedCertificate(cert.getSerialNumber());
    return entry != null;
  }

  private X509CRL fetchCrl(String crlUrl) {
    try {
      logger.debug("Fetching CRL from {}", crlUrl);

      URL url = new URL(crlUrl);
      HttpGet get = new HttpGet(url.toString());
      try (CloseableHttpResponse response = this.httpClient.execute(get)) {
        try (InputStream inputStream = response.getEntity().getContent()) {
          CertificateFactory cf = CertificateFactory.getInstance("X.509");
          return (X509CRL) cf.generateCRL(inputStream);
        }
      }
    } catch (IOException | CRLException | CertificateException e) {
      logger.debug("Failed to fetch CRL from {}: {}", crlUrl, e.getMessage());
      return null;
    }
  }

  private List<String> extractCRLDistributionPoints(X509Certificate cert) {
    List<String> crlUrls = new ArrayList<>();

    try {
      byte[] extensionBytes = cert.getExtensionValue("2.5.29.31");
      if (extensionBytes == null) {
        logger.debug(
            "No CRL Distribution Points extension found for certificate: {}",
            cert.getSubjectX500Principal());
        return crlUrls;
      }

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

    // Add 1 minute margin to handle inclusive time range of notBefore/notAfter
    maximumValidityPeriodMs += 60 * 1000;

    long certValidityPeriodMs = cert.getNotAfter().getTime() - cert.getNotBefore().getTime();
    return maximumValidityPeriodMs > certValidityPeriodMs;
  }

  private boolean containsOnlyRevokedChains(List<CRLValidationResult> results) {
    return !results.isEmpty()
        && results.stream().allMatch(result -> result == CRLValidationResult.CHAIN_REVOKED);
  }

  private boolean verifyIssuingDistributionPoint(X509CRL crl, X509Certificate cert, String crlUrl) {
    try {
      byte[] extensionBytes = crl.getExtensionValue("2.5.29.28");
      if (extensionBytes == null) {
        logger.debug("No IDP extension found - CRL covers all certificates");
        return true;
      }

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

      DistributionPointName dpName = idp.getDistributionPoint();
      if (dpName != null) {
        if (dpName.getType() == DistributionPointName.FULL_NAME) {
          GeneralNames generalNames = (GeneralNames) dpName.getName();
          boolean foundMatch = false;

          for (GeneralName generalName : generalNames.getNames()) {
            if (generalName.getTagNo() == GeneralName.uniformResourceIdentifier) {
              String idpUrl = ((DERIA5String) generalName.getName()).getString();
              if (idpUrl.equals(crlUrl)) {
                foundMatch = true;
                break;
              }
            }
          }

          if (!foundMatch) {
            logger.debug(
                "CRL URL {} not found in IDP distribution points - this CRL is not authorized for this certificate",
                crlUrl);
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
