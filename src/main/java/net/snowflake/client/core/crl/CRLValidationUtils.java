package net.snowflake.client.core.crl;

import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
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

class CRLValidationUtils {

  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLValidationUtils.class);

  // CA/Browser Forum Baseline Requirements date thresholds (using UTC for consistency)
  private static final Date MARCH_15_2024 =
      Date.from(LocalDate.of(2024, 3, 15).atStartOfDay(ZoneId.of("UTC")).toInstant());
  private static final Date MARCH_15_2026 =
      Date.from(LocalDate.of(2026, 3, 15).atStartOfDay(ZoneId.of("UTC")).toInstant());

  static List<String> extractCRLDistributionPoints(X509Certificate cert) {
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
      CRLDistPoint crlDistPoint =
          CRLDistPoint.getInstance(ASN1Primitive.fromByteArray(octetString.getOctets()));

      DistributionPoint[] distributionPoints = crlDistPoint.getDistributionPoints();
      if (distributionPoints != null) {
        for (DistributionPoint dp : distributionPoints) {
          DistributionPointName dpName = dp.getDistributionPoint();
          if (dpName != null && dpName.getType() == DistributionPointName.FULL_NAME) {
            GeneralNames generalNames = (GeneralNames) dpName.getName();
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
   * Determines if a certificate is short-lived according to CA/Browser Forum Baseline Requirements.
   */
  static boolean isShortLived(X509Certificate cert) {
    Date notBefore = cert.getNotBefore();
    Date notAfter = cert.getNotAfter();

    // Certificates issued before March 15, 2024 are not considered short-lived
    if (notBefore.before(MARCH_15_2024)) {
      return false;
    }

    // Determine the maximum validity period based on issuance date
    long maxValidityPeriodMs;
    if (notBefore.before(MARCH_15_2026)) {
      maxValidityPeriodMs =
          10L * 24 * 60 * 60 * 1000; // 10 days for certificates before March 15, 2026
    } else {
      maxValidityPeriodMs =
          7L * 24 * 60 * 60 * 1000; // 7 days for certificates after March 15, 2026
    }

    // Add 1 minute margin to account for clock differences and inclusive time boundaries
    maxValidityPeriodMs += 60 * 1000;

    long actualValidityPeriodMs = notAfter.getTime() - notBefore.getTime();
    return actualValidityPeriodMs <= maxValidityPeriodMs;
  }

  static boolean verifyIssuingDistributionPoint(X509CRL crl, X509Certificate cert, String crlUrl) {
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

  static String getCertChainSubjects(List<X509Certificate[]> certificateChains) {
    return certificateChains.stream()
        .flatMap(Arrays::stream)
        .map(cert -> cert.getSubjectX500Principal().getName())
        .collect(Collectors.joining(", "));
  }
}
