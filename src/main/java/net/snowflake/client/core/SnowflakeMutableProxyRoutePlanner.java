package net.snowflake.client.core;

import java.io.Serializable;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.protocol.HttpContext;

/**
 * This class defines a ProxyRoutePlanner (used for creating HttpClients) that has the ability to
 * change the nonProxyHosts setting.
 */
public class SnowflakeMutableProxyRoutePlanner extends DefaultRoutePlanner
    implements HttpRoutePlanner, Serializable {
  private HttpHost proxy;
  private String nonProxyHosts;
  private String[] hostPatterns;

  /**
   * @param host host
   * @param proxyPort proxy port
   * @param proxyProtocol proxy protocol
   * @param nonProxyHosts non-proxy hosts
   */
  public SnowflakeMutableProxyRoutePlanner(
      String host, int proxyPort, HttpProtocol proxyProtocol, String nonProxyHosts) {
    super(DefaultSchemePortResolver.INSTANCE);
    this.proxy = new HttpHost(host, proxyPort, proxyProtocol.toString());
    this.nonProxyHosts = nonProxyHosts;
    parseNonProxyHosts(nonProxyHosts);
  }

  private void parseNonProxyHosts(String nonProxyHosts) {
    if (!SnowflakeUtil.isNullOrEmpty(nonProxyHosts)) {
      String[] hosts = nonProxyHosts.split("\\|");
      hostPatterns = new String[hosts.length];
      for (int i = 0; i < hosts.length; ++i) {
        hostPatterns[i] = hosts[i].toLowerCase().replace("*", ".*?");
      }
    }
  }

  boolean doesTargetMatchNonProxyHosts(HttpHost target) {
    if (hostPatterns == null) {
      return false;
    }
    String targetHost = target.getHostName().toLowerCase();
    for (String pattern : hostPatterns) {
      if (targetHost.matches(pattern)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Set non-proxy hosts
   *
   * @param nonProxyHosts non-proxy hosts
   */
  public void setNonProxyHosts(String nonProxyHosts) {
    this.nonProxyHosts = nonProxyHosts;
    parseNonProxyHosts(nonProxyHosts);
  }

  /**
   * @return non-proxy hosts string
   */
  public String getNonProxyHosts() {
    return nonProxyHosts;
  }

  @Override
  protected HttpHost determineProxy(HttpHost target, HttpRequest request, HttpContext context) {
    return doesTargetMatchNonProxyHosts(target) ? null : proxy;
  }
}
