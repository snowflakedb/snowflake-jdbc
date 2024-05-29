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
        super("SSL/TLS Certificate Test");
    }

    @Override
    public void run(SnowflakeEndpoint snowflakeEndpoint){
        super.run(snowflakeEndpoint);
        if (snowflakeEndpoint.isSslEnabled()) {
            try {
                SSLSocketFactory sslsocketfactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                SSLSocket sslsocket = (SSLSocket) sslsocketfactory
                        .createSocket(snowflakeEndpoint.getHost(), snowflakeEndpoint.getPort());
                SSLSession session = sslsocket.getSession();
                Certificate[] chainCerts = session.getPeerCertificates();

                logger.debug("Printing certificate chain");
                int i = 0;
                for (Certificate cert : chainCerts) {
                    logger.debug("Certificate[{}]:", i);
                    if (cert.getType().equals("X.509")){
                        X509Certificate x509Cert = (X509Certificate) cert;
                        logger.debug("Subject: {} \n" +
                        "Issuer: {} \n" +
                        "Valid from: {} \n" +
                        "Not Valid After: {} \n" +
                        "Subject Alternative Names: {} \n" +
                        "Issuer Alternative Names: {} \n" +
                        "Serial: {} \n",
                                x509Cert.getSubjectDN()
                                ,x509Cert.getIssuerDN()
                                ,x509Cert.getNotBefore()
                                ,x509Cert.getNotAfter()
                                ,x509Cert.getSubjectAlternativeNames()
                                ,x509Cert.getIssuerAlternativeNames()
                                ,x509Cert.getSerialNumber()
                        );
                    }
                    else
                        logger.debug("Certificate Type is not x509: {} ", cert.getType());
                    i++;
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
