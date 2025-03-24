package net.snowflake.client.core;

import com.amazonaws.Protocol;
import com.amazonaws.http.apache.SdkProxyRoutePlanner;
import java.io.Serializable;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.protocol.HttpContext;

/**
 * This class defines a ProxyRoutePlanner (used for creating HttpClients) that has the ability to
 * change the nonProxyHosts setting.
 */
public class SnowflakeMutableProxyRoutePlanner implements HttpRoutePlanner, Serializable {

  private SdkProxyRoutePlanner proxyRoutePlanner = null;
  private String host;
  private int proxyPort;
  private String nonProxyHosts;
  private HttpProtocol protocol;

  /**
   * @deprecated Use {@link #SnowflakeMutableProxyRoutePlanner(String, int, HttpProtocol, String)}
   *     instead
   * @param host host
   * @param proxyPort proxy port
   * @param proxyProtocol proxy protocol
   * @param nonProxyHosts non-proxy hosts
   */
  @Deprecated
  public SnowflakeMutableProxyRoutePlanner(
      String host, int proxyPort, Protocol proxyProtocol, String nonProxyHosts) {
    this(host, proxyPort, toSnowflakeProtocol(proxyProtocol), nonProxyHosts);
  }

  /**
   * @param host host
   * @param proxyPort proxy port
   * @param proxyProtocol proxy protocol
   * @param nonProxyHosts non-proxy hosts
   */
  public SnowflakeMutableProxyRoutePlanner(
      String host, int proxyPort, HttpProtocol proxyProtocol, String nonProxyHosts) {
    proxyRoutePlanner =
        new SdkProxyRoutePlanner(host, proxyPort, toAwsProtocol(proxyProtocol), nonProxyHosts);
    this.host = host;
    this.proxyPort = proxyPort;
    this.nonProxyHosts = nonProxyHosts;
    this.protocol = proxyProtocol;
  }

  /**
   * Set non-proxy hosts
   *
   * @param nonProxyHosts non-proxy hosts
   */
  public void setNonProxyHosts(String nonProxyHosts) {
    this.nonProxyHosts = nonProxyHosts;
    proxyRoutePlanner =
        new SdkProxyRoutePlanner(host, proxyPort, toAwsProtocol(protocol), nonProxyHosts);
  }

  /**
   * @return non-proxy hosts string
   */
  public String getNonProxyHosts() {
    return nonProxyHosts;
  }

  @Override
  public HttpRoute determineRoute(HttpHost target, HttpRequest request, HttpContext context)
      throws HttpException {
    return proxyRoutePlanner.determineRoute(target, request, context);
  }

  private static Protocol toAwsProtocol(HttpProtocol protocol) {
    return protocol == HttpProtocol.HTTP ? Protocol.HTTP : Protocol.HTTPS;
  }

  private static HttpProtocol toSnowflakeProtocol(Protocol protocol) {
    return protocol == Protocol.HTTP ? HttpProtocol.HTTP : HttpProtocol.HTTPS;
  }
}
