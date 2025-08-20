package net.snowflake.client.core.crl;

import static net.snowflake.client.core.crl.CRLValidationUtils.extractCRLDistributionPoints;
import static net.snowflake.client.core.crl.CRLValidationUtils.isShortLived;
import static net.snowflake.client.core.crl.CRLValidationUtils.verifyIssuingDistributionPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.crl.CertificateGeneratorUtil.CertificateChain;
import org.bouncycastle.asn1.x509.IssuingDistributionPoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
class CRLValidationUtilsTest {
  private CertificateGeneratorUtil certGen;

  @BeforeEach
  void setUp() {
    certGen = new CertificateGeneratorUtil();
  }

  @Nested
  class ExtractCRLDistributionPointsTests {

    @Test
    void shouldExtractMultipleCRLDistributionPoints() throws Exception {
      List<String> expectedUrls =
          Arrays.asList(
              "http://crl.snowflake.com/test.crl", "https://backup-crl.snowflake.com/test.crl");

      X509Certificate cert =
          certGen.createCertificateWithCRLDistributionPoints("CN=Test Certificate", expectedUrls);
      List<String> extractedUrls = extractCRLDistributionPoints(cert);

      assertEquals(2, extractedUrls.size(), "Should extract 2 CRL URLs");
      assertTrue(extractedUrls.contains("http://crl.snowflake.com/test.crl"));
      assertTrue(extractedUrls.contains("https://backup-crl.snowflake.com/test.crl"));
    }

    @Test
    void shouldReturnEmptyListForCertificateWithoutCRLDistributionPoints() throws Exception {
      CertificateChain chain = certGen.createSimpleChain();
      List<String> extractedUrls = extractCRLDistributionPoints(chain.leafCert);

      assertTrue(extractedUrls.isEmpty());
    }

    @Test
    void shouldFilterOnlyHttpAndHttpsUrls() throws Exception {
      List<String> mixedUrls =
          Arrays.asList(
              "http://crl.snowflake.com/test.crl",
              "https://secure-crl.snowflake.com/test.crl",
              "HTTP://crl2.snowflake.com/test.crl",
              "HTTPS://secure-crl2.snowflake.com/test.crl",
              "ftp://ftp.snowflake.com/test.crl", // Should be filtered out
              "FTP://ftp2.snowflake.com/test.crl", // Should be filtered out
              "ldap://ldap.snowflake.com/test.crl"); // Should be filtered out

      X509Certificate cert =
          certGen.createCertificateWithCRLDistributionPoints("CN=Test Certificate", mixedUrls);
      List<String> extractedUrls = extractCRLDistributionPoints(cert);

      assertEquals(4, extractedUrls.size(), "Should extract only HTTP/HTTPS URLs");
      assertTrue(extractedUrls.contains("http://crl.snowflake.com/test.crl"));
      assertTrue(extractedUrls.contains("HTTP://crl2.snowflake.com/test.crl"));
      assertTrue(extractedUrls.contains("https://secure-crl.snowflake.com/test.crl"));
      assertTrue(extractedUrls.contains("HTTPS://secure-crl2.snowflake.com/test.crl"));
      assertFalse(extractedUrls.contains("ftp://ftp.snowflake.com/test.crl"));
      assertFalse(extractedUrls.contains("FTP://ftp2.snowflake.com/test.crl"));
      assertFalse(extractedUrls.contains("ldap://ldap.snowflake.com/test.crl"));
    }

    @Test
    void shouldHandleEmptyCRLDistributionPointsList() throws Exception {
      X509Certificate cert =
          certGen.createCertificateWithCRLDistributionPoints(
              "CN=Test Certificate", Arrays.asList());
      List<String> extractedUrls = extractCRLDistributionPoints(cert);

      assertTrue(extractedUrls.isEmpty());
    }
  }

  @Nested
  class IsShortLivedTests {
    @Test
    void shouldNotConsiderCertificatesIssuedBeforeMarch2024AsShortLived() throws Exception {
      Date feb2024Date =
          Date.from(LocalDate.of(2024, 2, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
      X509Certificate cert = certGen.createShortLivedCertificate(5, feb2024Date);

      assertFalse(isShortLived(cert));
    }

    @Test
    void shouldConsiderShortCertificatesIssuedAfterMarch2024AsShortLived() throws Exception {
      Date april2024Date =
          Date.from(LocalDate.of(2024, 4, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
      X509Certificate cert = certGen.createShortLivedCertificate(5, april2024Date);

      assertTrue(isShortLived(cert));
    }

    @Test
    void shouldApply10DayThresholdBetweenMarch2024AndMarch2026() throws Exception {
      Date april2024Date =
          Date.from(LocalDate.of(2024, 4, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());

      assertTrue(isShortLived(certGen.createShortLivedCertificate(10, april2024Date)));
      assertFalse(isShortLived(certGen.createShortLivedCertificate(15, april2024Date)));
    }

    @Test
    void shouldApply7DayThresholdAfterMarch2026() throws Exception {
      Date april2026Date =
          Date.from(LocalDate.of(2026, 4, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());

      X509Certificate shortCert = certGen.createShortLivedCertificate(6, april2026Date);
      assertTrue(isShortLived(shortCert));

      X509Certificate longCert = certGen.createShortLivedCertificate(8, april2026Date);
      assertFalse(isShortLived(longCert));
    }

    @Test
    void shouldHandleEdgeCaseWithExact7DaysAfter2026() throws Exception {
      Date april2026Date =
          Date.from(LocalDate.of(2026, 4, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
      X509Certificate cert = certGen.createShortLivedCertificate(7, april2026Date);

      assertTrue(isShortLived(cert));
    }
  }

  @Nested
  class VerifyIssuingDistributionPointTests {
    @Test
    void shouldReturnTrueForCRLWithoutIDPExtension() throws Exception {
      CertificateChain chain = certGen.createSimpleChain();
      X509CRL crl = certGen.createCRLWithIDP(chain.rootCert, null);

      assertTrue(
          verifyIssuingDistributionPoint(crl, chain.leafCert, "http://snowflake.com/test.crl"));
    }

    @Test
    void shouldRejectUserCertificateWhenCRLOnlyCoversUserCerts() throws Exception {
      CertificateChain chain = certGen.createSimpleChain();
      IssuingDistributionPoint idp =
          new IssuingDistributionPoint(null, true, false, null, false, false);
      X509CRL crl = certGen.createCRLWithIDP(chain.rootCert, idp);

      assertFalse(
          verifyIssuingDistributionPoint(
              crl, chain.intermediateCert, "http://snowflake.com/test.crl"),
          "Should reject CA certificate when CRL only covers user certificates");

      assertTrue(
          verifyIssuingDistributionPoint(crl, chain.leafCert, "http://snowflake.com/test.crl"),
          "Should accept end-entity certificate when CRL only covers user certificates");
    }

    @Test
    void shouldRejectEndEntityCertificateWhenCRLOnlyCoversCACs() throws Exception {
      CertificateChain chain = certGen.createSimpleChain();
      IssuingDistributionPoint idp =
          new IssuingDistributionPoint(null, false, true, null, false, false);
      X509CRL crl = certGen.createCRLWithIDP(chain.rootCert, idp);

      assertFalse(
          verifyIssuingDistributionPoint(crl, chain.leafCert, "http://snowflake.com/test.crl"),
          "Should reject end-entity certificate when CRL only covers CA certificates");

      assertTrue(
          verifyIssuingDistributionPoint(
              crl, chain.intermediateCert, "http://snowflake.com/test.crl"),
          "Should accept CA certificate when CRL only covers CA certificates");
    }

    @Test
    void shouldValidateURLMatchingInIDPDistributionPoints() throws Exception {
      CertificateChain chain = certGen.createSimpleChain();
      String crlUrl = "http://crl.snowflake.com/test.crl";
      List<String> idpUrls = Arrays.asList(crlUrl, "http://backup.snowflake.com/test.crl");

      X509CRL crl = certGen.createCRLWithIDPDistributionPoints(chain.rootCert, idpUrls);

      assertTrue(verifyIssuingDistributionPoint(crl, chain.leafCert, crlUrl));
    }

    @Test
    void shouldRejectURLNotMatchingIDPDistributionPoints() throws Exception {
      CertificateChain chain = certGen.createSimpleChain();
      String unauthorizedUrl = "http://unauthorized.snowflake.com/test.crl";
      List<String> idpUrls =
          Arrays.asList(
              "http://authorized.snowflake.com/test.crl", "http://backup.snowflake.com/test.crl");

      X509CRL crl = certGen.createCRLWithIDPDistributionPoints(chain.rootCert, idpUrls);

      assertFalse(verifyIssuingDistributionPoint(crl, chain.leafCert, unauthorizedUrl));
    }

    @Test
    void shouldRejectWhenIDPHasEmptyDistributionPointsList() throws Exception {
      CertificateChain chain = certGen.createSimpleChain();
      X509CRL crl = certGen.createCRLWithIDPDistributionPoints(chain.rootCert, Arrays.asList());

      assertFalse(
          verifyIssuingDistributionPoint(crl, chain.leafCert, "http://snowflake.com/test.crl"));
    }

    @Test
    void shouldHandleIDPWithoutDistributionPoints() throws Exception {
      CertificateChain chain = certGen.createSimpleChain();
      IssuingDistributionPoint idp =
          new IssuingDistributionPoint(null, false, false, null, false, false);
      X509CRL crl = certGen.createCRLWithIDP(chain.rootCert, idp);

      assertTrue(
          verifyIssuingDistributionPoint(crl, chain.leafCert, "http://snowflake.com/test.crl"));
    }
  }
}
