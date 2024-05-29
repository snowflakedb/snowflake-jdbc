package net.snowflake.client.diagnostic;

import static org.junit.Assert.*;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.category.TestCategoryDiagnostic;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.diagnostic.DiagnosticContext;
import net.snowflake.client.jdbc.diagnostic.SnowflakeEndpoint;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryDiagnostic.class)
public class DiagnosticContextLatestIT {

  private static final String HTTP_NON_PROXY_HOSTS = "http.nonProxyHosts";
  private static final String HTTP_PROXY_HOST = "http.proxyHost";
  private static final String HTTP_PROXY_PORT = "http.proxyPort";
  private static final String HTTPS_PROXY_HOST = "https.proxyHost";
  private static final String HTTPS_PROXY_PORT = "https.proxyPort";

  /*
  Check that all the mock Snowflake Endpoints we manually created exist in the array returned to us by
  the DiagnosticContext class which it generated after it parsed the allowlist.json file during initialization.
  */
  @Test
  public void parseAllowListFileTest() {
    Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();
    File allowlistFile = new File("src/test/resources/allowlist.json");

    DiagnosticContext diagnosticContext =
        new DiagnosticContext(allowlistFile.getAbsolutePath(), connectionPropertiesMap);
    ArrayList<SnowflakeEndpoint> endpointsFromTestFile = diagnosticContext.getEndpoints();
    ArrayList<SnowflakeEndpoint> mockEndpoints = new ArrayList<>();

    mockEndpoints.add(
        new SnowflakeEndpoint("SNOWFLAKE_DEPLOYMENT", "account_name.snowflakecomputing.com", 443));
    mockEndpoints.add(
        new SnowflakeEndpoint(
            "SNOWFLAKE_DEPLOYMENT_REGIONLESS", "org-account_name.snowflakecomputing.com", 443));
    mockEndpoints.add(new SnowflakeEndpoint("STAGE", "stage-bucket.s3.amazonaws.com", 443));
    mockEndpoints.add(
        new SnowflakeEndpoint("STAGE", "stage-bucket.s3.us-west-2.amazonaws.com", 443));
    mockEndpoints.add(
        new SnowflakeEndpoint("STAGE", "stage-bucket.s3-us-west-2.amazonaws.com", 443));
    mockEndpoints.add(
        new SnowflakeEndpoint("SNOWSQL_REPO", "snowsql_repo.snowflakecomputing.com", 443));
    mockEndpoints.add(
        new SnowflakeEndpoint(
            "OUT_OF_BAND_TELEMETRY", "out_of_band_telemetry.snowflakecomputing.com", 443));
    mockEndpoints.add(new SnowflakeEndpoint("OCSP_CACHE", "ocsp_cache.snowflakecomputing.com", 80));
    mockEndpoints.add(new SnowflakeEndpoint("DUO_SECURITY", "duo_security.duosecurity.com", 443));
    mockEndpoints.add(new SnowflakeEndpoint("OCSP_RESPONDER", "ocsp.rootg2.amazontrust.com", 80));
    mockEndpoints.add(new SnowflakeEndpoint("OCSP_RESPONDER", "o.ss2.us", 80));
    mockEndpoints.add(new SnowflakeEndpoint("OCSP_RESPONDER", "ocsp.sca1b.amazontrust.com", 80));
    mockEndpoints.add(new SnowflakeEndpoint("OCSP_RESPONDER", "ocsp.r2m01.amazontrust.com", 80));
    mockEndpoints.add(new SnowflakeEndpoint("OCSP_RESPONDER", "ocsp.rootca1.amazontrust.com", 80));
    mockEndpoints.add(
        new SnowflakeEndpoint("SNOWSIGHT_DEPLOYMENT", "snowsight_deployment.snowflake.com", 443));
    mockEndpoints.add(
        new SnowflakeEndpoint("SNOWSIGHT_DEPLOYMENT", "snowsight_deployment_2.snowflake.com", 443));

    String testFailedMessage =
        "The lists of SnowflakeEndpoints in mockEndpoints and endpointsFromTestFile should be identical";
    assertTrue(testFailedMessage, endpointsFromTestFile.containsAll(mockEndpoints));
  }

  /*
  Test that we correctly determine that proxy settings are absent from both the JVM and
  the connections parameters (i.e. empty strings for hostnames, or -1 for ports).
   */
  @Test
  public void testEmptyProxyConfig() {
    Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();

    DiagnosticContext diagnosticContext = new DiagnosticContext(connectionPropertiesMap);

    assertFalse("Proxy configurations should be empty", diagnosticContext.isProxyEnabled());
    assertTrue(
        "getHttpProxyHost() must return an empty string in the absence of proxy configuration",
        diagnosticContext.getHttpProxyHost().isEmpty());
    assertEquals(
        "getHttpProxyPort() must return -1 in the absence of proxy configuration",
        -1,
        diagnosticContext.getHttpProxyPort());
    assertTrue(
        "getHttpsProxyHost() must return an empty string in the absence of proxy configuration",
        diagnosticContext.getHttpsProxyHost().isEmpty());
    assertEquals(
        "getHttpsProxyPort() must return -1 in the absence of proxy configuration",
        -1,
        diagnosticContext.getHttpsProxyPort());
    assertTrue(
        "getHttpNonProxyHosts() must return an empty string in the absence of proxy configuration",
        diagnosticContext.getHttpNonProxyHosts().isEmpty());
  }

  @Test
  public void testProxyConfigSetOnJvm() {
    System.setProperty(HTTP_PROXY_HOST, "http.proxyHost.com");
    System.setProperty(HTTP_PROXY_PORT, "8080");
    System.setProperty(HTTPS_PROXY_HOST, "https.proxyHost.com");
    System.setProperty(HTTPS_PROXY_PORT, "8083");
    System.setProperty(HTTP_NON_PROXY_HOSTS, "*.domain.com|localhost");

    Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();

    DiagnosticContext diagnosticContext = new DiagnosticContext(connectionPropertiesMap);

    System.clearProperty(HTTP_PROXY_HOST);
    System.clearProperty(HTTP_PROXY_PORT);
    System.clearProperty(HTTPS_PROXY_HOST);
    System.clearProperty(HTTPS_PROXY_PORT);
    System.clearProperty(HTTP_NON_PROXY_HOSTS);

    assertTrue(diagnosticContext.isProxyEnabled());
    assertTrue(diagnosticContext.isProxyEnabledOnJvm());
    assertEquals(diagnosticContext.getHttpProxyHost(), "http.proxyHost.com");
    assertEquals(diagnosticContext.getHttpProxyPort(), 8080);
    assertEquals(diagnosticContext.getHttpsProxyHost(), "https.proxyHost.com");
    assertEquals(diagnosticContext.getHttpsProxyPort(), 8083);
    assertEquals(diagnosticContext.getHttpNonProxyHosts(), "*.domain.com|localhost");
  }

  /*
  If Proxy settings are passed using JVM arguments and connection parameters
  then the connection parameters take precedence.
   */
  @Test
  public void testProxyOverrideWithConnectionParameter() {

    System.setProperty(HTTP_PROXY_HOST, "http.proxyHost.com");
    System.setProperty(HTTP_PROXY_PORT, "8080");
    System.setProperty(HTTPS_PROXY_HOST, "https.proxyHost.com");
    System.setProperty(HTTPS_PROXY_PORT, "8083");
    System.setProperty(HTTP_NON_PROXY_HOSTS, "*.domain.com|localhost");

    Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();

    connectionPropertiesMap.put(SFSessionProperty.PROXY_HOST, "override.proxyHost.com");
    connectionPropertiesMap.put(SFSessionProperty.PROXY_PORT, "80");
    connectionPropertiesMap.put(SFSessionProperty.NON_PROXY_HOSTS, "*.new_domain.com|localhost");

    DiagnosticContext diagnosticContext = new DiagnosticContext(connectionPropertiesMap);

    System.clearProperty(HTTP_PROXY_HOST);
    System.clearProperty(HTTP_PROXY_PORT);
    System.clearProperty(HTTPS_PROXY_HOST);
    System.clearProperty(HTTPS_PROXY_PORT);
    System.clearProperty(HTTP_NON_PROXY_HOSTS);

    assertTrue(diagnosticContext.isProxyEnabled());
    assertFalse(diagnosticContext.isProxyEnabledOnJvm());
    assertEquals(diagnosticContext.getHttpProxyHost(), "override.proxyHost.com");
    assertEquals(diagnosticContext.getHttpProxyPort(), 80);
    assertEquals(diagnosticContext.getHttpsProxyHost(), "override.proxyHost.com");
    assertEquals(diagnosticContext.getHttpsProxyPort(), 80);
    assertEquals(diagnosticContext.getHttpNonProxyHosts(), "*.new_domain.com|localhost");
  }

  @Test
  public void testGetProxy() {
    System.setProperty(HTTP_PROXY_HOST, "http.proxyHost.com");
    System.setProperty(HTTP_PROXY_PORT, "8080");
    System.setProperty(HTTPS_PROXY_HOST, "https.proxyHost.com");
    System.setProperty(HTTPS_PROXY_PORT, "8083");
    System.setProperty(HTTP_NON_PROXY_HOSTS, "*.domain.com|localhost|*.snowflakecomputing.com");

    Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();

    DiagnosticContext diagnosticContext = new DiagnosticContext(connectionPropertiesMap);

    System.clearProperty(HTTP_PROXY_HOST);
    System.clearProperty(HTTP_PROXY_PORT);
    System.clearProperty(HTTPS_PROXY_HOST);
    System.clearProperty(HTTPS_PROXY_PORT);
    System.clearProperty(HTTP_NON_PROXY_HOSTS);

    String httpProxyHost = diagnosticContext.getHttpProxyHost();
    int httpProxyPort = diagnosticContext.getHttpProxyPort();
    String httpsProxyHost = diagnosticContext.getHttpsProxyHost();
    int httpsProxyPort = diagnosticContext.getHttpsProxyPort();

    SnowflakeEndpoint httpsHostBypassingProxy =
        new SnowflakeEndpoint("SNOWFLAKE_DEPLOYMENT", "account_name.snowflakecomputing.com", 443);
    SnowflakeEndpoint httpHostBypassingProxy =
        new SnowflakeEndpoint("OCSP_CACHE", "ocsp_cache.snowflakecomputing.com", 80);
    SnowflakeEndpoint hostWithHttpProxy =
        new SnowflakeEndpoint("OCSP_RESPONDER", "ocsp.rootg2.amazontrust.com", 80);
    SnowflakeEndpoint hostWithHttpsProxy =
        new SnowflakeEndpoint("STAGE", "stage-bucket.s3-us-west-2.amazonaws.com", 443);

    Proxy byPassProxy = Proxy.NO_PROXY;
    Proxy httpProxy =
        new Proxy(Proxy.Type.HTTP, new InetSocketAddress(httpProxyHost, httpProxyPort));
    Proxy httpsProxy =
        new Proxy(Proxy.Type.HTTP, new InetSocketAddress(httpsProxyHost, httpsProxyPort));

    assertEquals(byPassProxy, diagnosticContext.getProxy(httpsHostBypassingProxy));
    assertEquals(byPassProxy, diagnosticContext.getProxy(httpHostBypassingProxy));
    assertEquals(httpProxy, diagnosticContext.getProxy(hostWithHttpProxy));
    assertEquals(httpsProxy, diagnosticContext.getProxy(hostWithHttpsProxy));
  }

  /*
  Test that we correctly create direct HTTPS connections and only route HTTP requests through a proxy server
  when we set only the -Dhttp.proxyHost and -Dhttp.proxyPort arguments
  */
  @Test
  public void testGetHttpProxyOnly() {
    System.setProperty(HTTP_PROXY_HOST, "http.proxyHost.com");
    System.setProperty(HTTP_PROXY_PORT, "8080");

    Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();

    DiagnosticContext diagnosticContext = new DiagnosticContext(connectionPropertiesMap);

    System.clearProperty(HTTP_PROXY_HOST);
    System.clearProperty(HTTP_PROXY_PORT);

    String httpProxyHost = diagnosticContext.getHttpProxyHost();
    int httpProxyPort = diagnosticContext.getHttpProxyPort();

    Proxy noProxy = Proxy.NO_PROXY;
    Proxy httpProxy =
        new Proxy(Proxy.Type.HTTP, new InetSocketAddress(httpProxyHost, httpProxyPort));

    SnowflakeEndpoint httpsHostDirectConnection =
        new SnowflakeEndpoint("SNOWFLAKE_DEPLOYMENT", "account_name.snowflakecomputing.com", 443);
    SnowflakeEndpoint httpHostProxy =
        new SnowflakeEndpoint("OCSP_CACHE", "ocsp_cache.snowflakecomputing.com", 80);

    assertEquals(noProxy, diagnosticContext.getProxy(httpsHostDirectConnection));
    assertEquals(httpProxy, diagnosticContext.getProxy(httpHostProxy));
  }

  /*
  Test that we correctly create direct HTTP connections and only route HTTPS through a proxy server
  when we set only the -Dhttps.proxyHost and -Dhttps.proxyPort parameters
  */
  @Test
  public void testGetHttpsProxyOnly() {
    System.setProperty(HTTPS_PROXY_HOST, "https.proxyHost.com");
    System.setProperty(HTTPS_PROXY_PORT, "8083");

    Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();

    DiagnosticContext diagnosticContext = new DiagnosticContext(connectionPropertiesMap);

    System.clearProperty(HTTPS_PROXY_HOST);
    System.clearProperty(HTTPS_PROXY_PORT);

    String httpsProxyHost = diagnosticContext.getHttpsProxyHost();
    int httpsProxyPort = diagnosticContext.getHttpsProxyPort();

    Proxy noProxy = Proxy.NO_PROXY;
    Proxy httpsProxy =
        new Proxy(Proxy.Type.HTTP, new InetSocketAddress(httpsProxyHost, httpsProxyPort));

    SnowflakeEndpoint httpsHostProxy =
        new SnowflakeEndpoint("SNOWFLAKE_DEPLOYMENT", "account_name.snowflakecomputing.com", 443);
    SnowflakeEndpoint httpHostDirectConnection =
        new SnowflakeEndpoint("OCSP_CACHE", "ocsp_cache.snowflakecomputing.com", 80);

    assertEquals(noProxy, diagnosticContext.getProxy(httpHostDirectConnection));
    assertEquals(httpsProxy, diagnosticContext.getProxy(httpsHostProxy));
  }
}
