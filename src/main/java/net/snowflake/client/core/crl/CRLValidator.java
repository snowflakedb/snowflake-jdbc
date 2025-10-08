package net.snowflake.client.core.crl;

import static net.snowflake.client.core.crl.CRLValidationUtils.extractCRLDistributionPoints;
import static net.snowflake.client.core.crl.CRLValidationUtils.getCertChainSubjects;
import static net.snowflake.client.core.crl.CRLValidationUtils.isShortLived;
import static net.snowflake.client.core.crl.CRLValidationUtils.verifyIssuingDistributionPoint;

import java.io.ByteArrayInputStream;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.telemetry.PreSessionTelemetryClient;
import net.snowflake.client.jdbc.telemetry.RevocationCheckTelemetryData;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

@SnowflakeJdbcInternalApi
public class CRLValidator {
  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLValidator.class);
  private static final Map<HttpClientSettingsKey, CRLValidator> validatorRegistryForTelemetry =
      new ConcurrentHashMap<>();

  private final Map<String, Lock> urlLocks = new ConcurrentHashMap<>();
  private final CloseableHttpClient httpClient;
  private final CRLCacheManager cacheManager;
  private final CertRevocationCheckMode certRevocationCheckMode;
  private final boolean allowCertificatesWithoutCrlUrl;
  private final Telemetry telemetryClient;

  public CRLValidator(
      CertRevocationCheckMode revocationCheckMode,
      boolean allowCertificatesWithoutCrlUrl,
      CloseableHttpClient httpClient,
      CRLCacheManager cacheManager,
      Telemetry telemetryClient) {
    this.httpClient = httpClient;
    this.cacheManager = cacheManager;
    this.certRevocationCheckMode = revocationCheckMode;
    this.allowCertificatesWithoutCrlUrl = allowCertificatesWithoutCrlUrl;
    this.telemetryClient = telemetryClient;
  }

  /**
   * Validates certificate chains against CRLs.
   *
   * @param certificateChains the verified certificate chains to validate
   * @return true if validation passes, false otherwise
   */
  public boolean validateCertificateChains(List<X509Certificate[]> certificateChains) {
    if (this.certRevocationCheckMode == CertRevocationCheckMode.DISABLED) {
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
    if (this.certRevocationCheckMode == CertRevocationCheckMode.ADVISORY) {
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
          if (this.allowCertificatesWithoutCrlUrl) {
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
      Instant now = Instant.now();
      RevocationCheckTelemetryData revocationTelemetry = new RevocationCheckTelemetryData();
      revocationTelemetry.setCrlUrl(crlUrl);

      CRLCacheEntry cacheEntry = cacheManager.get(crlUrl);
      X509CRL crl = cacheEntry != null ? cacheEntry.getCrl() : null;
      Instant downloadTime = cacheEntry != null ? cacheEntry.getDownloadTime() : null;

      boolean needsFreshCrl =
          crl == null
              || (crl.getNextUpdate() != null && crl.getNextUpdate().toInstant().isBefore(now))
              || cacheEntry.isEvicted(now, cacheManager.getCacheValidityTime());

      boolean shouldUpdateCache = false;

      if (needsFreshCrl) {
        X509CRL newCrl = fetchCrl(crlUrl, revocationTelemetry);

        if (newCrl != null) {
          shouldUpdateCache = crl == null || newCrl.getThisUpdate().after(crl.getThisUpdate());

          if (shouldUpdateCache) {
            logger.debug("Found updated CRL for {}", crlUrl);
            crl = newCrl;
            downloadTime = now;
          } else {
            // New CRL isn't newer, check if old one is still valid
            if (crl.getNextUpdate() != null && crl.getNextUpdate().toInstant().isAfter(now)) {
              logger.debug("CRL for {} is up-to-date, using cached version", crlUrl);
            } else {
              logger.warn("CRL for {} is not available or outdated", crlUrl);
              return CertificateValidationResult.CERT_ERROR;
            }
          }
        } else {
          if (crl != null
              && crl.getNextUpdate() != null
              && crl.getNextUpdate().toInstant().isAfter(now)) {
            logger.debug(
                "Using cached CRL for {} (fetch failed but cached version still valid)", crlUrl);
          } else {
            logger.error(
                "Unable to fetch fresh CRL from {} and no valid cached version available", crlUrl);
            return CertificateValidationResult.CERT_ERROR;
          }
        }
      }

      int numberOfRevokedCertificates =
          crl.getRevokedCertificates() != null ? crl.getRevokedCertificates().size() : 0;
      logger.debug(
          "CRL has {} revoked entries, next update at {}",
          numberOfRevokedCertificates,
          crl.getNextUpdate());
      revocationTelemetry.setNumberOfRevokedCertificates(numberOfRevokedCertificates);

      if (!isCrlSignatureAndIssuerValid(crl, cert, parentCert, crlUrl)) {
        logger.debug("Unable to verify CRL for {}", crlUrl);
        return CertificateValidationResult.CERT_ERROR;
      }

      // Update cache if we have a new/updated CRL
      if (shouldUpdateCache) {
        logger.debug("CRL for {} is valid, updating cache", crlUrl);
        cacheManager.put(crlUrl, crl, downloadTime);
      }

      if (isCertificateRevoked(crl, cert)) {
        logger.debug(
            "Certificate {} is revoked according to CRL {}", cert.getSerialNumber(), crlUrl);
        return CertificateValidationResult.CERT_REVOKED;
      }

      telemetryClient.addLogToBatch(revocationTelemetry.buildTelemetry());
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

  private X509CRL fetchCrl(String crlUrl, RevocationCheckTelemetryData revocationTelemetry) {
    try {
      logger.debug("Fetching CRL from {}", crlUrl);
      URL url = new URL(crlUrl);
      HttpGet get = new HttpGet(url.toString());
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      long start = System.currentTimeMillis();
      try (CloseableHttpResponse response = this.httpClient.execute(get)) {
        try (InputStream inputStream = response.getEntity().getContent()) {
          byte[] crlData = IOUtils.toByteArray(inputStream);
          revocationTelemetry.setTimeDownloadingCrl(System.currentTimeMillis() - start);
          start = System.currentTimeMillis();
          X509CRL crl = (X509CRL) cf.generateCRL(new ByteArrayInputStream(crlData));
          long crlBytes = crl.getEncoded().length;
          revocationTelemetry.setTimeParsingCrl(System.currentTimeMillis() - start);
          revocationTelemetry.setCrlBytes(crlBytes);
          return crl;
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

  /**
   * Multiple sessions may share the same HttpClientSettingsKey thus CRL telemetry might be sent for
   * wrong session. We accept this limitation.
   */
  public static void setTelemetryClientForKey(
      HttpClientSettingsKey key, Telemetry telemetryClient) {
    CRLValidator result =
        validatorRegistryForTelemetry.computeIfPresent(
            key,
            (k, validator) -> {
              validator.provideTelemetryClient(telemetryClient);
              return validator;
            });

    if (result == null) {
      logger.debug("No CRL validator found for key: {}", key);
    }
  }

  public static void registerValidator(HttpClientSettingsKey key, CRLValidator validator) {
    validatorRegistryForTelemetry.put(key, validator);
  }

  private void provideTelemetryClient(Telemetry telemetryClient) {
    try {
      PreSessionTelemetryClient preSessionTelemetryClient =
          (PreSessionTelemetryClient) this.telemetryClient;
      if (!preSessionTelemetryClient.hasRealTelemetryClient()) {
        preSessionTelemetryClient.setRealTelemetryClient(telemetryClient);
      }
    } catch (Exception e) {
      logger.warn("Failed to set real telemetry client for trust manager: {}", e.getMessage());
    }
  }
}
