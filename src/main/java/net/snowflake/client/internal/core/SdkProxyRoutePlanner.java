package net.snowflake.client.internal.core;

import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.protocol.HttpContext;

/**
 * SdkProxyRoutePlanner delegates a Proxy Route Planner from the settings instead of the system
 * properties. It will use the proxy created from proxyHost, proxyPort, and proxyProtocol and filter
 * the hosts who matches nonProxyHosts pattern.
 */
public class SdkProxyRoutePlanner extends DefaultRoutePlanner {
  private HttpHost proxy;
  private String[] hostPatterns;

  public SdkProxyRoutePlanner(
      String proxyHost, int proxyPort, HttpProtocol proxyProtocol, String nonProxyHosts) {
    super(DefaultSchemePortResolver.INSTANCE);
    proxy = new HttpHost(proxyHost, proxyPort, proxyProtocol.toString());
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

  @Override
  protected HttpHost determineProxy(
      final HttpHost target, final HttpRequest request, final HttpContext context)
      throws HttpException {

    return doesTargetMatchNonProxyHosts(target) ? null : proxy;
  }
}
