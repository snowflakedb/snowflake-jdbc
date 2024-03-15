package net.snowflake.client.jdbc.diagnostic;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

public class CertificateDiagnosticCheck extends DiagnosticCheck {

    private static final SFLogger logger =
            SFLoggerFactory.getLogger(CertificateDiagnosticCheck.class);

    public CertificateDiagnosticCheck(){
        super("SSL/TLS Certificate Diagnostic Test");
    }

    @Override
    public void run(SnowflakeEndpoint snowflakeEndpoint){
        if (snowflakeEndpoint.isSslEnabled()) {
            try {
                SSLSocketFactory sslsocketfactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                SSLSocket sslsocket = (SSLSocket) sslsocketfactory
                        .createSocket(snowflakeEndpoint.getHost(), snowflakeEndpoint.getPort());
                SSLSession session = sslsocket.getSession();
                Certificate[] chaincerts = session.getPeerCertificates();

                for (Certificate cert : chaincerts) {
                    if (cert.getType() == "X.509"){
                        logger.debug("Certificate Type is ok: " + cert.getType());
                        X509Certificate x509Cert = (X509Certificate) cert;
                        logger.debug("Subject: " + x509Cert.getSubjectDN());
                        logger.debug("Issuer: " + x509Cert.getIssuerDN());
                        logger.debug("Valid from: " + x509Cert.getNotBefore());
                        logger.debug("Not Valid After: " + x509Cert.getNotAfter());
                        logger.debug("Subject Alternative Names: " + x509Cert.getSubjectAlternativeNames());
                        logger.debug("Issuer Alternative Names: " + x509Cert.getIssuerAlternativeNames());
                        logger.debug("Serial: " + x509Cert.getSerialNumber());
                    }
                    else
                        logger.debug("Certificate Type is not x509: " + cert.getType());
                }
            }catch(Exception e){
                logger.debug("SSL/TLS Certificate check failed");
                logger.error(e.getMessage(), e);
            }
        }
        else {
            logger.debug("Host " + snowflakeEndpoint.getHost() + ":" + snowflakeEndpoint.getPort() + " is not secure. Skipping certificate check.");
        }

    }

}
