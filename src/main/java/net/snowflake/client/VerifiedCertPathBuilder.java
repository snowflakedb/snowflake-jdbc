package net.snowflake.client;

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
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
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * A class that builds and verifies certificate paths using a truststore and CertPathBuilder. This
 * class takes a certificate chain presented by a server and returns verified paths that can be
 * trusted based on the configured truststore.
 */
public class VerifiedCertPathBuilder {

  private static final SFLogger logger = SFLoggerFactory.getLogger(VerifiedCertPathBuilder.class);
  private final X509TrustManager trustManager;

  /**
   * Constructor that initializes the VerifiedCertPathBuilder with the default system truststore.
   *
   * @throws CertificateException if there's an error initializing the trust manager
   */
  public VerifiedCertPathBuilder() throws CertificateException {
    this.trustManager = initializeTrustManager();
  }

  /**
   * Builds and verifies ALL possible certificate paths from leaf certificates to trust anchors.
   * This method focuses on end-entity certificates (leaf certificates) and discovers all valid
   * certification paths through any available intermediates, including cross-signed scenarios. This
   * approach is optimized for server certificate validation while maintaining complete coverage of
   * alternative paths that may be needed for CRL validation scenarios.
   *
   * @param certificateChain the certificate chain presented by the server
   * @return a list of all verified certificate paths from leaf certificates to trust anchors
   * @throws CertificateException if certificate validation fails
   * @throws CertPathBuilderException if no valid certificate paths could be built
   */
  public List<X509Certificate[]> buildAllVerifiedPaths(X509Certificate[] certificateChain)
      throws CertificateException, CertPathBuilderException {

    if (certificateChain == null || certificateChain.length == 0) {
      throw new IllegalArgumentException("Certificate chain cannot be null or empty");
    }

    logger.debug(
        "Building ALL verified paths for certificate chain of length: {}", certificateChain.length);

    List<X509Certificate[]> allVerifiedPaths = new ArrayList<>();

    try {
      // Create trust anchors from the truststore
      Set<TrustAnchor> trustAnchors = createTrustAnchors();

      // Create a certificate store containing the chain certificates
      Collection<Certificate> certCollection = new ArrayList<>();
      Collections.addAll(certCollection, certificateChain);
      CertStore certStore =
          CertStore.getInstance("Collection", new CollectionCertStoreParameters(certCollection));

      // Build PKIX parameters
      PKIXBuilderParameters pkixParams = new PKIXBuilderParameters(trustAnchors, null);
      pkixParams.addCertStore(certStore);
      pkixParams.setRevocationEnabled(false);

      // Identify leaf certificates (end-entity certificates that don't issue other certificates in
      // the chain)
      // This optimization focuses only on certificates that represent the actual server
      // certificate,
      // while still discovering all cross-signed paths through different intermediates
      List<X509Certificate> leafCertificates = identifyLeafCertificates(certificateChain);

      logger.debug(
          "Identified {} leaf certificate(s) from chain of length {}",
          leafCertificates.size(),
          certificateChain.length);

      // Try to build paths for each leaf certificate only
      // The CertPathBuilder will automatically discover all valid paths through any available
      // intermediates to any reachable trust anchor, including cross-signed scenarios
      for (X509Certificate leafCert : leafCertificates) {

        // Set the leaf certificate as the target for path building
        pkixParams.setTargetCertConstraints(
            new java.security.cert.X509CertSelector() {
              {
                setCertificate(leafCert);
              }
            });

        // Find all possible paths from this leaf certificate to trust anchors
        allVerifiedPaths.addAll(
            findAllPathsForTarget(leafCert, pkixParams, trustAnchors, certStore));
      }
    } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
      throw new CertificateException("Failed to build certificate paths", e);
    }

    if (allVerifiedPaths.isEmpty()) {
      throw new CertPathBuilderException("No valid certificate paths could be built");
    }

    logger.debug(
        "Successfully built {} unique verified certificate paths", allVerifiedPaths.size());
    return allVerifiedPaths;
  }

  /**
   * Finds all possible valid paths from a leaf certificate to trust anchors. This method explores
   * all available trust anchors and automatically discovers paths through any intermediate
   * certificates in the certificate store, including cross-signed intermediates that lead to
   * different trust anchors.
   */
  private List<X509Certificate[]> findAllPathsForTarget(
      X509Certificate targetCert,
      PKIXBuilderParameters pkixParams,
      Set<TrustAnchor> trustAnchors,
      CertStore certStore)
      throws CertificateException {

    List<X509Certificate[]> pathsForTarget = new ArrayList<>();

    // Try building paths with different trust anchor combinations
    for (TrustAnchor trustAnchor : trustAnchors) {
      try {
        // Create parameters with single trust anchor to force specific path discovery
        Set<TrustAnchor> singleTrustAnchor = Collections.singleton(trustAnchor);
        PKIXBuilderParameters singleAnchorParams =
            new PKIXBuilderParameters(singleTrustAnchor, null);
        singleAnchorParams.addCertStore(certStore);
        singleAnchorParams.setRevocationEnabled(false);
        singleAnchorParams.setTargetCertConstraints(
            new java.security.cert.X509CertSelector() {
              {
                setCertificate(targetCert);
              }
            });

        CertPathBuilder builder = CertPathBuilder.getInstance("PKIX");
        CertPathBuilderResult result = builder.build(singleAnchorParams);

        if (result instanceof PKIXCertPathBuilderResult) {
          PKIXCertPathBuilderResult pkixResult = (PKIXCertPathBuilderResult) result;
          CertPath certPath = pkixResult.getCertPath();

          try {
            // Convert CertPath to X509Certificate array
            X509Certificate[] certArray = convertCertPathToArray(certPath);

            // Validate the built path using the trust manager
            trustManager.checkServerTrusted(certArray, "RSA");
            pathsForTarget.add(certArray);
            logger.trace(
                "Found valid path for target {} via trust anchor {}: path length {}",
                targetCert.getSubjectX500Principal(),
                trustAnchor.getTrustedCert().getSubjectX500Principal(),
                certArray.length);
          } catch (CertificateException e) {
            logger.trace(
                "Path validation failed for target {} via trust anchor {}: {}",
                targetCert.getSubjectX500Principal(),
                trustAnchor.getTrustedCert().getSubjectX500Principal(),
                e.getMessage());
            // Continue with next trust anchor
          }
        }

      } catch (CertPathBuilderException
          | NoSuchAlgorithmException
          | InvalidAlgorithmParameterException e) {
        logger.trace(
            "Failed to build path for target {} via trust anchor {}: {}",
            targetCert.getSubjectX500Principal(),
            trustAnchor.getTrustedCert().getSubjectX500Principal(),
            e.getMessage());
        // Continue with next trust anchor
      }
    }

    return pathsForTarget;
  }

  /**
   * Identifies leaf certificates (end-entity certificates) in the certificate chain. A leaf
   * certificate is one that doesn't issue any other certificate in the chain. This optimization
   * focuses path building on actual server certificates while still allowing CertPathBuilder to
   * discover all cross-signed paths automatically.
   *
   * @param certificateChain the certificate chain to analyze
   * @return list of leaf certificates found in the chain
   */
  private List<X509Certificate> identifyLeafCertificates(X509Certificate[] certificateChain) {
    List<X509Certificate> leafCertificates = new ArrayList<>();

    for (X509Certificate cert : certificateChain) {
      boolean isIssuer = false;

      // Check if this certificate issues any other certificate in the chain
      for (X509Certificate otherCert : certificateChain) {
        if (!cert.equals(otherCert)
            && otherCert.getIssuerX500Principal().equals(cert.getSubjectX500Principal())) {
          isIssuer = true;
          break;
        }
      }

      // If this certificate doesn't issue any other certificate, it's a leaf
      if (!isIssuer) {
        leafCertificates.add(cert);
        logger.trace("Identified leaf certificate: {}", cert.getSubjectX500Principal());
      } else {
        logger.trace("Skipping intermediate certificate: {}", cert.getSubjectX500Principal());
      }
    }

    // Fallback: if no leaf certificates found (shouldn't happen), use the first certificate
    if (leafCertificates.isEmpty()) {
      logger.warn("No leaf certificates identified, falling back to first certificate");
      leafCertificates.add(certificateChain[0]);
    }

    return leafCertificates;
  }

  /**
   * Creates trust anchors from the truststore.
   *
   * @return a set of trust anchors
   * @throws CertificateException if there's an error accessing the truststore
   */
  private Set<TrustAnchor> createTrustAnchors() throws CertificateException {
    Set<TrustAnchor> trustAnchors = new HashSet<>();

    try {
      // Get trusted certificates from the trust manager
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

  /**
   * Initializes the X509TrustManager with the configured truststore. This method respects the JVM
   * truststore properties if set.
   *
   * @return the initialized trust manager
   * @throws CertificateException if initialization fails
   */
  private X509TrustManager initializeTrustManager() throws CertificateException {

    try {
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

      // Initialize with null - this automatically honors javax.net.ssl.trustStore properties
      // and falls back to system default truststore if no custom truststore is configured
      tmf.init((KeyStore) null);

      for (javax.net.ssl.TrustManager tm : tmf.getTrustManagers()) {
        if (tm instanceof X509TrustManager) {
          X509TrustManager x509TrustManager = (X509TrustManager) tm;
          logger.debug(
              "Initialized X509TrustManager with {} trusted certificates",
              x509TrustManager.getAcceptedIssuers().length);
          return x509TrustManager;
        }
      }

      throw new CertificateException("No X509TrustManager found");

    } catch (Exception e) {
      throw new CertificateException("Failed to initialize trust manager", e);
    }
  }

  /**
   * Converts a CertPath to an X509Certificate array.
   *
   * @param certPath the CertPath to convert
   * @return an array of X509Certificate objects
   * @throws CertificateException if the path is empty or contains non-X509 certificates
   */
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
            "Certificate path contains non-X509 certificate: " + cert.getClass().getName());
      }
      certArray[i] = (X509Certificate) cert;
    }

    return certArray;
  }
}
