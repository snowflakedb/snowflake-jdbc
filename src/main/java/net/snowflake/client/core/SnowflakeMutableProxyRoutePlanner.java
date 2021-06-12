/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

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

  public SnowflakeMutableProxyRoutePlanner(String host, int proxyPort, String nonProxyHosts) {
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
