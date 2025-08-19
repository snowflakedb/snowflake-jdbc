package net.snowflake.client.core.crl;

import static net.snowflake.client.core.crl.CRLValidationUtils.extractCRLDistributionPoints;
import static net.snowflake.client.core.crl.CRLValidationUtils.getCertChainSubjects;
import static net.snowflake.client.core.crl.CRLValidationUtils.isShortLived;
import static net.snowflake.client.core.crl.CRLValidationUtils.verifyIssuingDistributionPoint;

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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

class CRLValidator {
  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLValidator.class);
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

    logger.debug(
        "Validating {} certificate chains with subjects: {}",
        certificateChains.size(),
        getCertChainSubjects(certificateChains));

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

    Set<CertificateValidationResult> results = new HashSet<>();

    for (String url : crlUrls) {
      CertificateValidationResult result = validateCert(cert, url, parentCert);

      if (result == CertificateValidationResult.CERT_REVOKED) {
        return result;
      }

      results.add(result);
    }

    if (results.contains(CertificateValidationResult.CERT_ERROR)) {
      return CertificateValidationResult.CERT_ERROR;
    } else {
      return CertificateValidationResult.CERT_UNREVOKED;
    }
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

  private boolean containsOnlyRevokedChains(List<CRLValidationResult> results) {
    return !results.isEmpty()
        && results.stream().allMatch(result -> result == CRLValidationResult.CHAIN_REVOKED);
  }
}
