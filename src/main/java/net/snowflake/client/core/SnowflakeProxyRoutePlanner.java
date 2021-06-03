package net.snowflake.client.core;

import com.amazonaws.http.apache.SdkProxyRoutePlanner;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.protocol.HttpContext;

public class SnowflakeProxyRoutePlanner implements HttpRoutePlanner {

  private SdkProxyRoutePlanner proxyRoutePlanner = null;
  private String host;
  private int proxyPort;

  public SnowflakeProxyRoutePlanner(String host, int proxyPort, String nonProxyHosts) {
    proxyRoutePlanner = new SdkProxyRoutePlanner(host, proxyPort, nonProxyHosts);
    this.host = host;
    this.proxyPort = proxyPort;
  }

  public void setNonProxyHosts(String nonProxyHosts) {
    proxyRoutePlanner = new SdkProxyRoutePlanner(host, proxyPort, nonProxyHosts);
  }

  @Override
  public HttpRoute determineRoute(HttpHost target, HttpRequest request, HttpContext context)
      throws HttpException {
    return proxyRoutePlanner.determineRoute(target, request, context);
  }
}
