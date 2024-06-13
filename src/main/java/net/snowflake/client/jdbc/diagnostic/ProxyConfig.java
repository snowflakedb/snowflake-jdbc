package net.snowflake.client.jdbc.diagnostic;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.regex.Pattern;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * This class is used to represent the proxy configurations passed to the JDBC driver either as JVM
 * arguments or connection parameters. The class determines which proxy settings take precedence and
 * should be used by the diagnostic tests. We normalize configurations where empty strings for
 * hostnames and -1 for ports represent the absence of a configuration.
 *
 * <p>The order of precedence is:
 *
 * <p>1.) Connection parameters (proxy configurations passed to the constructor) 2.) JVM arguments
 *
 * <p>The useProxy parameter is ignored. If the proxy is configured using the JVM and someone wants
 * to bypass that at the connection-level then they would need to set the following connection
 * parameters: proxyHost=127.0.0.1 proxyPort=8080 nonProxyHosts=*
 *
 * <p>i.e. bypass the proxy host when connecting to any host.
 */
class ProxyConfig {
  private String proxyHost;
  private int proxyPort;
  private String nonProxyHosts;
  private String jvmHttpProxyHost;
  private String jvmHttpsProxyHost;
  private int jvmHttpProxyPort;
  private int jvmHttpsProxyPort;
  private String jvmNonProxyHosts;
  private String finalHttpProxyHost = "";
  private String finalHttpsProxyHost = "";
  private int finalHttpProxyPort = -1;
  private int finalHttpsProxyPort = -1;
  private String finalNonProxyHosts = "";
  private boolean isProxyEnabled = false;

  private boolean isProxyEnabledOnJvm = false;

  private final String JVM_HTTP_PROXY_HOST = "http.proxyHost";
  private final String JVM_HTTPS_PROXY_HOST = "https.proxyHost";
  private final String JVM_HTTP_PROXY_PORT = "http.proxyPort";
  private final String JVM_HTTPS_PROXY_PORT = "https.proxyPort";
  private final String JVM_HTTP_NON_PROXY_HOSTS = "http.nonProxyHosts";

  private static final SFLogger logger = SFLoggerFactory.getLogger(ProxyConfig.class);

  public String getHttpProxyHost() {
    return finalHttpProxyHost;
  }

  public String getHttpsProxyHost() {
    return finalHttpsProxyHost;
  }

  public int getHttpProxyPort() {
    return finalHttpProxyPort;
  }

  public int getHttpsProxyPort() {
    return finalHttpsProxyPort;
  }

  public String getNonProxyHosts() {
    return finalNonProxyHosts;
  }

  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  public void setProxyPort(int proxyPort) {
    this.proxyPort = proxyPort;
  }

  public void setNonProxyHosts(String nonProxyHosts) {
    this.nonProxyHosts = nonProxyHosts;
  }

  public ProxyConfig(String proxyHost, int proxyPort, String nonProxyHosts) {
    jvmHttpProxyHost =
        (System.getProperty(JVM_HTTP_PROXY_HOST) == null)
            ? ""
            : System.getProperty(JVM_HTTP_PROXY_HOST);

    jvmHttpsProxyHost =
        (System.getProperty(JVM_HTTPS_PROXY_HOST) == null)
            ? ""
            : System.getProperty(JVM_HTTPS_PROXY_HOST);

    jvmHttpProxyPort =
        (System.getProperty(JVM_HTTP_PROXY_PORT) == null)
            ? -1
            : Integer.parseInt(System.getProperty(JVM_HTTP_PROXY_PORT));

    jvmHttpsProxyPort =
        (System.getProperty(JVM_HTTPS_PROXY_PORT) == null)
            ? -1
            : Integer.parseInt(System.getProperty(JVM_HTTPS_PROXY_PORT));

    jvmNonProxyHosts =
        (System.getProperty(JVM_HTTP_NON_PROXY_HOSTS) == null)
            ? ""
            : System.getProperty(JVM_HTTP_NON_PROXY_HOSTS);
    this.proxyHost = (proxyHost == null) ? "" : proxyHost;
    this.proxyPort = proxyPort;
    this.nonProxyHosts = (nonProxyHosts == null) ? "" : nonProxyHosts;
    resolveProxyConfigurations();
  }

  public ProxyConfig() {
    this(null, -1, null);
  }

  public boolean isProxyEnabled() {
    return isProxyEnabled;
  }

  public boolean isProxyEnabledOnJvm() {
    return isProxyEnabledOnJvm;
  }

  /**
   * This method reviews both the JVM and connection parameter configurations then concludes which
   * settings to use 1.) Check if proxy settings were passed in the connection parameters, if so,
   * then we use that right away. 2.) If connection parameters were not passed, then review JVM
   * arguments and use those. 3.) If neither were set, then don't use any proxy settings (default).
   */
  private void resolveProxyConfigurations() {
    // Both proxyHost and proxyPort connection parameters must be present.
    StringBuilder sb = new StringBuilder();
    logger.info("Resolving proxy configurations");
    sb.append("Proxy Configurations picked up from ");
    if (!proxyHost.isEmpty() && proxyPort != -1) {
      finalHttpProxyHost = proxyHost;
      finalHttpsProxyHost = proxyHost;
      finalHttpProxyPort = proxyPort;
      finalHttpsProxyPort = proxyPort;
      finalNonProxyHosts = nonProxyHosts;
      isProxyEnabled = true;
      sb.append("connection parameters:\n");
      sb.append("proxyHost: ").append(proxyHost).append("\n");
      sb.append("proxyPort: ").append(proxyPort).append("\n");
      sb.append("nonProxyHosts: ").append(nonProxyHosts);
    } else if ((!jvmHttpProxyHost.isEmpty() && jvmHttpProxyPort != -1)
        || (!jvmHttpsProxyHost.isEmpty() && jvmHttpsProxyPort != -1)) {
      finalHttpProxyHost = jvmHttpProxyHost;
      finalHttpProxyPort = jvmHttpProxyPort;
      finalHttpsProxyHost = jvmHttpsProxyHost;
      finalHttpsProxyPort = jvmHttpsProxyPort;
      finalNonProxyHosts = jvmNonProxyHosts;
      isProxyEnabled = true;
      isProxyEnabledOnJvm = true;
      sb.append("JVM arguments:\n");
      sb.append("-D").append(JVM_HTTP_PROXY_HOST).append("=").append(jvmHttpProxyHost).append("\n");
      sb.append("-D").append(JVM_HTTP_PROXY_PORT).append("=").append(jvmHttpProxyPort).append("\n");
      sb.append("-D")
          .append(JVM_HTTPS_PROXY_HOST)
          .append("=")
          .append(jvmHttpsProxyHost)
          .append("\n");
      sb.append("-D")
          .append(JVM_HTTPS_PROXY_PORT)
          .append("=")
          .append(jvmHttpsProxyPort)
          .append("\n");
    }
    logger.info(sb.toString());
  }

  protected boolean isBypassProxy(String hostname) {
    String nonProxyHosts = getNonProxyHosts().replace(".", "\\.").replace("*", ".*");
    String[] nonProxyHostsArray = nonProxyHosts.split("\\|");
    for (String i : nonProxyHostsArray) {
      if (Pattern.compile(i).matcher(hostname).matches()) {
        return true;
      }
    }
    return false;
  }

  public Proxy getProxy(SnowflakeEndpoint endpoint) {
    if (!isProxyEnabled || isBypassProxy(endpoint.getHost())) {
      return Proxy.NO_PROXY;
    } else if (endpoint.isSslEnabled()) {
      return (isHttpsProxyEnabled())
          ? new Proxy(
              Proxy.Type.HTTP, new InetSocketAddress(finalHttpsProxyHost, finalHttpsProxyPort))
          : Proxy.NO_PROXY;
    }
    return (isHttpProxyEnabled())
        ? new Proxy(Proxy.Type.HTTP, new InetSocketAddress(finalHttpProxyHost, finalHttpProxyPort))
        : Proxy.NO_PROXY;
  }

  /*
  Check that both http proxy host and http proxy port are set,
  only then do we consider that http proxy is enabled.
  */
  private boolean isHttpProxyEnabled() {
    return (!finalHttpProxyHost.isEmpty() || finalHttpProxyPort != -1);
  }

  /*
  Check that both https proxy host and http proxy port are set,
  only then do we consider that http proxy is enabled.
  */
  private boolean isHttpsProxyEnabled() {
    return (!finalHttpsProxyHost.isEmpty() || finalHttpsProxyPort != -1);
  }
}
