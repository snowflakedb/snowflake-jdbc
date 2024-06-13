package net.snowflake.client.jdbc.diagnostic;

import java.net.Socket;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class DiagnosticTrustManager extends X509ExtendedTrustManager {

  private static final SFLogger logger = SFLoggerFactory.getLogger(DiagnosticTrustManager.class);

  @Override
  public void checkServerTrusted(X509Certificate[] certs, String authType) {
    printCertificates(certs);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] certs, String authType, SSLEngine engine) {
    printCertificates(certs);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] certs, String authType, Socket sc) {
    printCertificates(certs);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType) {
    // do nothing
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, Socket sc) {
    // do nothing
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
    // do nothing
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    // This implementation is not needed, so we're returning an empty array
    return new X509Certificate[0];
  }

  private void printCertificates(X509Certificate[] chainCerts) {
    logger.info("Printing certificate chain");
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for (X509Certificate x509Cert : chainCerts) {
      try {
        sb.append("\nCertificate[").append(i).append("]:").append("\n");
        sb.append("Subject: ").append(x509Cert.getSubjectDN()).append("\n");
        sb.append("Issuer: ").append(x509Cert.getIssuerDN()).append("\n");
        sb.append("Valid from: ").append(x509Cert.getNotBefore()).append("\n");
        sb.append("Not Valid After: ").append(x509Cert.getNotAfter()).append("\n");
        sb.append("Subject Alternative Names: ")
            .append(x509Cert.getSubjectAlternativeNames())
            .append("\n");
        sb.append("Issuer Alternative Names: ")
            .append(x509Cert.getIssuerAlternativeNames())
            .append("\n");
        sb.append("Serial: ").append(x509Cert.getSerialNumber().toString(16)).append("\n");
        logger.info(sb.toString());
        i++;
      } catch (CertificateParsingException e) {
        logger.error("Error parsing certificate", e);
      } catch (Exception e) {
        logger.error("Unexpected error occurred when parsing certificate", e);
      }
    }
  }
}
