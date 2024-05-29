package net.snowflake.client.jdbc.diagnostic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.Proxy;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class DiagnosticContext {

  private static final SFLogger logger = SFLoggerFactory.getLogger(DiagnosticContext.class);
  private static final String JAVAX_NET_DEBUG = "javax.net.debug";

  private ProxyConfig proxyConf;
  String jsonInputFile;

  private ArrayList<SnowflakeEndpoint> endpoints = new ArrayList<>();

  private final DiagnosticCheck[] tests;

  public DiagnosticContext(
      String allowListFile, Map<SFSessionProperty, Object> connectionPropertiesMap) {

    createProxyConfiguration(connectionPropertiesMap);
    this.jsonInputFile = allowListFile;

    try {
      JsonNode jsonNode = readAllowListJsonFile(jsonInputFile);
      for (JsonNode objectNode : jsonNode) {
        String type = objectNode.get("type").asText();
        String host = objectNode.get("host").asText();
        int port = objectNode.get("port").asInt();
        SnowflakeEndpoint e = new SnowflakeEndpoint(type, host, port);
        endpoints.add(e);
      }

    } catch (IOException e) {
      logger.error("Failed to read allowlist file: ", allowListFile);
      logger.error(e.getLocalizedMessage(), e);
    } catch (Exception e) {
      logger.error("Failed to parse data in allowlist file:", allowListFile);
      logger.error(e.getLocalizedMessage(), e);
    }

    tests =
        new DiagnosticCheck[] {
          new DnsDiagnosticCheck(proxyConf),
          new TcpDiagnosticCheck(proxyConf),
          new CertificateDiagnosticCheck(proxyConf),
          new HttpAndHttpsDiagnosticCheck(proxyConf)
        };
  }

  /** This constructor is only used for testing */
  public DiagnosticContext(Map<SFSessionProperty, Object> connectionPropertiesMap) {
    createProxyConfiguration(connectionPropertiesMap);

    tests =
        new DiagnosticCheck[] {
          new DnsDiagnosticCheck(proxyConf),
          new TcpDiagnosticCheck(proxyConf),
          new CertificateDiagnosticCheck(proxyConf),
          new HttpAndHttpsDiagnosticCheck(proxyConf)
        };
  }

  private void createProxyConfiguration(Map<SFSessionProperty, Object> connectionPropertiesMap) {
    String proxyHost = (String) connectionPropertiesMap.get(SFSessionProperty.PROXY_HOST);
    int proxyPort =
        (connectionPropertiesMap.get(SFSessionProperty.PROXY_PORT) == null)
            ? -1
            : Integer.parseInt((String) connectionPropertiesMap.get(SFSessionProperty.PROXY_PORT));
    String nonProxyHosts = (String) connectionPropertiesMap.get(SFSessionProperty.NON_PROXY_HOSTS);
    proxyConf = new ProxyConfig(proxyHost, proxyPort, nonProxyHosts);
  }

  public void runDiagnostics() {

    getEnvironmentInfo();

    // Loop through endpoints and run diagnostic test on each one of them
    for (DiagnosticCheck test : tests) {
      for (SnowflakeEndpoint endpoint : endpoints) {
        test.run(endpoint);
      }
    }
  }

  private JsonNode readAllowListJsonFile(String jsonFilePath) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    File allowListFile = new File(jsonFilePath);

    return objectMapper.readTree(allowListFile);
  }

  public void getEnvironmentInfo() {
    logger.debug("Getting environment information");
    logger.debug("Current truststore used: " + getTrustStoreLocation());
    logger.debug("-Dnetworkaddress.cache.ttl: " + System.getProperty("networkaddress.cache.ttl"));
    logger.debug(
        "-Dnetworkaddress.cache.negative.ttl: "
            + System.getProperty("networkaddress.cache.negative.ttl"));
    logger.debug("-Djavax.net.debug: " + System.getProperty(JAVAX_NET_DEBUG));
  }

  private boolean isNullOrEmpty(String a) {
    return a == null || a.isEmpty();
  }

  /*
  We determine the truststore being used based on the JSSE documentation:

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
  private String getTrustStoreLocation() {
    final String JAVAX_TRUSTSTORE = "javax.net.ssl.trustStore";
    String trustStore = System.getProperty(JAVAX_TRUSTSTORE);
    String javaHome = System.getProperty("java.home");
    Path javaSecurityPath = FileSystems.getDefault().getPath(javaHome, "/lib/security");
    logger.debug("JAVA_HOME: " + javaHome);

    if (isNullOrEmpty(trustStore)) {
      logger.debug("-D{} is null", JAVAX_TRUSTSTORE);
      Path jssecacertsPath =
          FileSystems.getDefault().getPath(javaSecurityPath.toString(), "jssecacerts");
      Path cacertsPath = FileSystems.getDefault().getPath(javaSecurityPath.toString(), "cacerts");

      logger.debug("Checking if jssecacerts or cacerts exist");
      if (Files.exists(jssecacertsPath)) {
        logger.debug(jssecacertsPath.toString() + " exists");
        trustStore = jssecacertsPath.toString();
      } else if (Files.exists(cacertsPath)) {
        logger.debug(cacertsPath.toString() + " exists");
        trustStore = cacertsPath.toString();
      }
    } else {
      logger.debug("-D{} is set by user: ", JAVAX_TRUSTSTORE);
    }
    return trustStore;
  }

  public String getHttpProxyHost() {
    return proxyConf.getHttpProxyHost();
  }

  public int getHttpProxyPort() {
    return proxyConf.getHttpProxyPort();
  }

  public String getHttpsProxyHost() {
    return proxyConf.getHttpsProxyHost();
  }

  public int getHttpsProxyPort() {
    return proxyConf.getHttpsProxyPort();
  }

  public String getHttpNonProxyHosts() {
    return proxyConf.getNonProxyHosts();
  }

  public ArrayList<SnowflakeEndpoint> getEndpoints() {
    return endpoints;
  }

  public Proxy getProxy(SnowflakeEndpoint snowflakeEndpoint) {
    return this.proxyConf.getProxy(snowflakeEndpoint);
  }

  public boolean isProxyEnabled() {
    return proxyConf.isProxyEnabled();
  }

  public boolean isProxyEnabledOnJvm() {
    return proxyConf.isProxyEnabledOnJvm();
  }
}
