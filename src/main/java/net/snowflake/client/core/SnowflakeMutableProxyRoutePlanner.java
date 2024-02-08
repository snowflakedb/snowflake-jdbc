/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.amazonaws.Protocol;
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

  private SnowflakeSdkProxyRoutePlanner proxyRoutePlanner = null;
  private String host;
  private int proxyPort;
  private String nonProxyHosts;
  private HttpProtocol protocol;

  /**
   * @deprecated use SnowflakeMutableProxyRoutePlanner(String host, int proxyPort, HttpProtocol protocol, String nonProxyHosts)
   */
  public SnowflakeMutableProxyRoutePlanner(
      String host, int proxyPort, Protocol proxyProtocol, String nonProxyHosts) {
    proxyRoutePlanner = new SnowflakeSdkProxyRoutePlanner(host, proxyPort, toSnowflakeProtocol(proxyProtocol), nonProxyHosts);
    this.host = host;
    this.proxyPort = proxyPort;
    this.nonProxyHosts = nonProxyHosts;
    this.protocol = toSnowflakeProtocol(proxyProtocol);
  }

  public SnowflakeMutableProxyRoutePlanner(
          String host, int proxyPort, HttpProtocol proxyProtocol, String nonProxyHosts) {
    proxyRoutePlanner = new SnowflakeSdkProxyRoutePlanner(host, proxyPort, proxyProtocol, nonProxyHosts);
    this.host = host;
    this.proxyPort = proxyPort;
    this.nonProxyHosts = nonProxyHosts;
    this.protocol = proxyProtocol;
  }

  public void setNonProxyHosts(String nonProxyHosts) {
    this.nonProxyHosts = nonProxyHosts;
    proxyRoutePlanner = new SnowflakeSdkProxyRoutePlanner(host, proxyPort, protocol, nonProxyHosts);
  }

  public String getNonProxyHosts() {
    return nonProxyHosts;
  }

  @Override
  public HttpRoute determineRoute(HttpHost target, HttpRequest request, HttpContext context)
      throws HttpException {
    return proxyRoutePlanner.determineRoute(target, request, context);
  }

  private HttpProtocol toSnowflakeProtocol(Protocol protocol) {
      return protocol == Protocol.HTTP ? HttpProtocol.HTTP : HttpProtocol.HTTPS;
  }
}
