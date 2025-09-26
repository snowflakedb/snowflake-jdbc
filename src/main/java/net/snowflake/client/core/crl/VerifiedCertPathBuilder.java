package net.snowflake.client.core.crl;

import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertPath;
import java.security.cert.CertPathBuilder;
import java.security.cert.CertPathBuilderException;
import java.security.cert.CertPathBuilderResult;
import java.security.cert.CertStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.PKIXCertPathBuilderResult;
import java.security.cert.TrustAnchor;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Builds and verifies certificate paths using a truststore and CertPathBuilder. This class takes a
 * certificate chain presented by a server and returns verified paths that include trust anchors for
 * CRL validation support.
 */
@SnowflakeJdbcInternalApi
public class VerifiedCertPathBuilder {

  private static final SFLogger logger = SFLoggerFactory.getLogger(VerifiedCertPathBuilder.class);
  private final X509TrustManager trustManager;
  private final Set<TrustAnchor> trustAnchors;

  /**
   * Constructor that initializes the VerifiedCertPathBuilder with the provided trust manager.
   *
   * @param trustManager the X509TrustManager to use for certificate validation
   * @throws IllegalArgumentException if trustManager is null
   */
  public VerifiedCertPathBuilder(X509TrustManager trustManager) throws CertificateException {
    if (trustManager == null) {
      throw new IllegalArgumentException("Trust manager cannot be null");
    }
    this.trustManager = trustManager;
    this.trustAnchors = createTrustAnchors(trustManager);
  }

  /**
   * Builds and verifies all possible certificate paths from leaf certificates to trust anchors.
   * Unlike standard PKIX path building, this method includes trust anchor certificates at the end
   * of each path for CRL validation support.
   *
   * @param certificateChain the certificate chain presented by the server
   * @param authType the authentication type used for the connection
   * @return a list of all verified certificate paths with trust anchors included
   * @throws CertificateException if certificate validation fails
   * @throws CertPathBuilderException if no valid certificate paths could be built
   */
  public List<X509Certificate[]> buildAllVerifiedPaths(
      X509Certificate[] certificateChain, String authType)
      throws CertificateException, CertPathBuilderException {

    if (certificateChain == null || certificateChain.length == 0) {
      throw new IllegalArgumentException("Certificate chain cannot be null or empty");
    }
    if (authType == null || authType.trim().isEmpty()) {
      throw new IllegalArgumentException("Authentication type cannot be null or empty");
    }

    logger.debug(
        "Building verified paths for chain length: {} with authType: {}",
        certificateChain.length,
        authType);

    List<X509Certificate[]> allVerifiedPaths = new ArrayList<>();

    try {
      List<Certificate> certCollection = Arrays.asList(certificateChain);
      CertStore certStore =
          CertStore.getInstance("Collection", new CollectionCertStoreParameters(certCollection));

      X509Certificate leafCertificate = identifyLeafCertificate(certificateChain);
      logger.debug("Identified leaf certificate: {}", leafCertificate.getSubjectX500Principal());

      allVerifiedPaths.addAll(
          findAllPathsForTarget(leafCertificate, trustAnchors, certStore, authType));

    } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
      throw new CertificateException("Failed to build certificate paths", e);
    }

    if (allVerifiedPaths.isEmpty()) {
      throw new CertPathBuilderException("No valid certificate paths could be built");
    }

    logger.debug("Successfully built {} verified certificate paths", allVerifiedPaths.size());
    return allVerifiedPaths;
  }

  /** Finds all possible valid paths from a leaf certificate to trust anchors. */
  private List<X509Certificate[]> findAllPathsForTarget(
      X509Certificate targetCert,
      Set<TrustAnchor> trustAnchors,
      CertStore certStore,
      String authType) {

    List<X509Certificate[]> pathsForTarget = new ArrayList<>();

    for (TrustAnchor trustAnchor : trustAnchors) {
      try {
        Set<TrustAnchor> singleTrustAnchor = Collections.singleton(trustAnchor);
        PKIXBuilderParameters singleAnchorParams =
            new PKIXBuilderParameters(singleTrustAnchor, null);
        singleAnchorParams.addCertStore(certStore);
        singleAnchorParams.setRevocationEnabled(false);
        X509CertSelector selector = new X509CertSelector();
        selector.setCertificate(targetCert);
        singleAnchorParams.setTargetCertConstraints(selector);

        CertPathBuilder builder = CertPathBuilder.getInstance("PKIX");
        CertPathBuilderResult result = builder.build(singleAnchorParams);

        if (result instanceof PKIXCertPathBuilderResult) {
          PKIXCertPathBuilderResult pkixResult = (PKIXCertPathBuilderResult) result;
          CertPath certPath = pkixResult.getCertPath();

          try {
            X509Certificate[] certArray = convertCertPathToArray(certPath);
            trustManager.checkServerTrusted(certArray, authType);

            // Create path with trust anchor included for CRL validation
            X509Certificate[] pathWithTrustAnchor = new X509Certificate[certArray.length + 1];
            System.arraycopy(certArray, 0, pathWithTrustAnchor, 0, certArray.length);
            pathWithTrustAnchor[certArray.length] = trustAnchor.getTrustedCert();

            pathsForTarget.add(pathWithTrustAnchor);
            logger.trace(
                "Found valid path via trust anchor {}: length {}",
                trustAnchor.getTrustedCert().getSubjectX500Principal(),
                pathWithTrustAnchor.length);

          } catch (CertificateException e) {
            logger.trace(
                "Path validation failed via trust anchor {}: {}",
                trustAnchor.getTrustedCert().getSubjectX500Principal(),
                e.getMessage());
          }
        }
      } catch (CertPathBuilderException
          | NoSuchAlgorithmException
          | InvalidAlgorithmParameterException e) {
        logger.trace(
            "Failed to build path via trust anchor {}: {}",
            trustAnchor.getTrustedCert().getSubjectX500Principal(),
            e.getMessage());
      }
    }

    return pathsForTarget;
  }

  /**
   * Identifies the leaf certificate (end-entity certificate) in the certificate chain.
   *
   * @param certificateChain the certificate chain to analyze
   * @return the leaf certificate found in the chain
   * @throws CertificateException if no leaf certificate is found in the chain
   */
  private X509Certificate identifyLeafCertificate(X509Certificate[] certificateChain)
      throws CertificateException {
    Set<X509Certificate> leafCerts =
        Arrays.stream(certificateChain)
            .filter(
                cert ->
                    cert != null
                        && cert.getBasicConstraints()
                            == -1) // Basic constraints -1 indicates a leaf certificate
            .collect(Collectors.toSet());
    if (leafCerts.isEmpty()) {
      throw new CertificateException("No leaf certificate found in the chain");
    }
    if (leafCerts.size() > 1) {
      throw new CertificateException("Multiple leaf certificates found");
    }
    return leafCerts.iterator().next();
  }

  /** Creates trust anchors from the truststore. */
  private Set<TrustAnchor> createTrustAnchors(X509TrustManager trustManager)
      throws CertificateException {
    Set<TrustAnchor> trustAnchors = new HashSet<>();

    try {
      X509Certificate[] trustedCerts = trustManager.getAcceptedIssuers();
      for (X509Certificate cert : trustedCerts) {
        trustAnchors.add(new TrustAnchor(cert, null));
      }
      logger.debug("Created {} trust anchors from truststore", trustAnchors.size());
    } catch (Exception e) {
      throw new CertificateException("Failed to create trust anchors", e);
    }

    return trustAnchors;
  }

  /** Converts a CertPath to an X509Certificate array. */
  private X509Certificate[] convertCertPathToArray(CertPath certPath) throws CertificateException {
    List<? extends Certificate> certificates = certPath.getCertificates();

    if (certificates == null || certificates.isEmpty()) {
      throw new CertificateException("Certificate path is empty");
    }

    X509Certificate[] certArray = new X509Certificate[certificates.size()];
    for (int i = 0; i < certificates.size(); i++) {
      Certificate cert = certificates.get(i);
      if (!(cert instanceof X509Certificate)) {
        throw new CertificateException(
            "Certificate path contains non-X509 certificate: "
                + cert.getClass().getCanonicalName());
      }
      certArray[i] = (X509Certificate) cert;
    }

    return certArray;
  }
}
