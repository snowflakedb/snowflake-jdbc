package net.snowflake.client.jdbc.diagnostic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class DiagnosticContext {

    private static final SFLogger logger =
            SFLoggerFactory.getLogger(DiagnosticContext.class);
    final static String JAVAX_NET_DEBUG = "javax.net.debug";
    String jsonInputFile;

    boolean enableSslTrace = false; //SSL tracing is off by default in case that's never provided by the user.
    ArrayList<SnowflakeEndpoint> endpoints = new ArrayList<>();

    DiagnosticCheck[] tests = new DiagnosticCheck[] {
            new DnsDiagnosticCheck(),
            new TcpDiagnosticCheck(),
            new CertificateDiagnosticCheck(),
            new HttpAndHttpsDiagnosticCheck()
    };

    public DiagnosticContext(String allowListFile, boolean enableSslTrace) {
        this.jsonInputFile = allowListFile;
        this.enableSslTrace = enableSslTrace;

        if(enableSslTrace){
                setSslDebugging(true);
        }
    }

    static class DiagnosticUtil {

    }

    public void runDiagnostics() {

        getEnvironmentInfo();

        try {
            JsonNode jsonNode = readAllowListJsonFile(jsonInputFile);
            for (JsonNode objectNode : jsonNode) {
                String type = objectNode.get("type").asText();
                String host = objectNode.get("host").asText();
                int port = objectNode.get("port").asInt();
                SnowflakeEndpoint e = new SnowflakeEndpoint(type, host, port);
                endpoints.add(e);
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        }

        //Loop through endpoints and run diangostic test on each one of them
        for(DiagnosticCheck test : tests) {
            for (SnowflakeEndpoint endpoint: endpoints) {
                test.run(endpoint);
            }
        }
    }
    /* Convenience method to use in order to enable SSL debugging
     through a connection parameter in case someone can't directly
     add a JVM argument to the process and can't restart it.
   */
    public void setSslDebugging(Boolean enable) {
        if (enable) {
            System.setProperty(JAVAX_NET_DEBUG, "all");
        }
    }

    private JsonNode readAllowListJsonFile(String jsonFilePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        File allowListFile = new File(jsonFilePath);

        return objectMapper.readTree(allowListFile);
    }

    public static void getEnvironmentInfo() {
        logger.debug("Getting environment information");
        logger.debug("Current truststore used: " + getTrustStoreLocation());
        logger.debug("-Dnetworkaddress.cache.ttl: " + System.getProperty("networkaddress.cache.ttl"));
        logger.debug("-Dnetworkaddress.cache.negative.ttl: " + System.getProperty("networkaddress.cache.negative.ttl"));
        logger.debug("-Djavax.net.debug: " + System.getProperty(JAVAX_NET_DEBUG));
    }

    public static boolean isNullOrEmpty(String a) {
        return a == null || a.isEmpty();
    }

    /*
  https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html

  1.) If the javax.net.ssl.trustStore property is defined, then the TrustManagerFactory attempts
  to find a file using the file name specified by that system property, and uses that file for the
  KeyStore parameter. If the javax.net.ssl.trustStorePassword system property is also defined,
  then its value is used to check the integrity of the data in the truststore before opening it.

  If the javax.net.ssl.trustStore property is defined but the specified file does not exist, then
  a default TrustManager using an empty keystore is created.

  2.) If the javax.net.ssl.trustStore system property was not specified, then:
    - if the file java-home/lib/security/jssecacerts exists, that file is used;
    - if the file java-home/lib/security/cacerts exists, that file is used;
    - if neither of these files exists, then the SSL cipher suite is anonymous,
      does not perform any authentication, and thus does not need a truststore.
  */
    private static String getTrustStoreLocation() {
        String trustStore = System.getProperty("javax.net.ssl.trustStore");
        String javaHome = System.getProperty("java.home");
        Path javaSecurityPath = FileSystems.getDefault().getPath(javaHome, "/lib/security");
        logger.debug("JAVA_HOME: " + javaHome);

        if (isNullOrEmpty(trustStore)) {
            logger.debug("-Djavax.net.ssl.trustStore is null");
            Path jssecacertsPath = FileSystems.getDefault().getPath(javaSecurityPath.toString(), "jssecacerts");
            Path cacertsPath = FileSystems.getDefault().getPath(javaSecurityPath.toString(), "cacerts");

            logger.debug("Checking if jssecacerts or cacerts exist");
            if (Files.exists(jssecacertsPath)) {
                logger.debug(jssecacertsPath.toString() + " exists");
                trustStore = jssecacertsPath.toString();
            }
            else if (Files.exists(cacertsPath)) {
                logger.debug(cacertsPath.toString() + " exists");
                trustStore = cacertsPath.toString();
            }
        }

        return trustStore;
    }

    boolean checkIfJvmSslTraceIsEnabled() {
        // Return true if the JVM argument -Djavax.net.debug is already set.
        return (System.getProperty(JAVAX_NET_DEBUG) != null);
    }
}
