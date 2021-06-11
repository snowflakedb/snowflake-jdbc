/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.google.common.base.Strings;
import java.io.Serializable;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

/**
 * This class defines all non-static parameters needed to create an HttpClient object. It is used as
 * the key for the static hashmap of reusable http clients.
 */
public class HttpClientSettingsKey implements Serializable {

  private OCSPMode ocspMode;
  private boolean useProxy;
  private String proxyHost = "";
  private int proxyPort = 0;
  private String nonProxyHosts = "";
  private String proxyUser = "";
  private String proxyPassword = "";
  private SnowflakeProxyRoutePlanner routePlanner = null;
  private CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

  public HttpClientSettingsKey(
      OCSPMode mode, String host, int port, String nonProxyHosts, String user, String password) {
    this.useProxy = true;
    this.ocspMode = mode != null ? mode : OCSPMode.FAIL_OPEN;
    this.proxyHost = host.trim();
    this.proxyPort = port;
    this.nonProxyHosts = nonProxyHosts.trim();
    this.proxyUser = user.trim();
    this.proxyPassword = password.trim();
  }

  public HttpClientSettingsKey(OCSPMode mode) {
    this.useProxy = false;
    this.ocspMode = mode != null ? mode : OCSPMode.FAIL_OPEN;
    ;
  }

  @Override
  public boolean equals(final Object O) {
    if (O instanceof HttpClientSettingsKey) {
      HttpClientSettingsKey comparisonKey = (HttpClientSettingsKey) O;
      if (comparisonKey.ocspMode.getValue() == this.ocspMode.getValue()) {
        if (!comparisonKey.useProxy) {
          return true;
        } else if (comparisonKey.proxyHost.equalsIgnoreCase(this.proxyHost)
            && comparisonKey.proxyPort == this.proxyPort
            && comparisonKey.proxyUser.equalsIgnoreCase(this.proxyUser)) {
          // update nonProxyHost and password if changed
          updateMutableFields(comparisonKey.proxyPassword, comparisonKey.nonProxyHosts);
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.ocspMode.getValue() + (this.proxyHost + this.proxyPort + this.proxyUser).hashCode();
  }

  public OCSPMode getOcspMode() {
    return this.ocspMode;
  }

  public boolean usesProxy() {
    return this.useProxy;
  }

  public String getProxyHost() {
    return this.proxyHost;
  }

  public int getProxyPort() {
    return this.proxyPort;
  }

  public String getProxyUser() {
    return this.proxyUser;
  }

  /** Be careful of using this! Should only be called when password is later masked. */
  public String getProxyPassword() {
    return this.proxyPassword;
  }

  public String getNonProxyHosts() {
    return this.nonProxyHosts;
  }

  public HttpHost getProxy() {
    HttpHost proxy = new HttpHost(this.proxyHost, this.proxyPort);
    return proxy;
  }

  public void updateMutableFields(String proxyPassword, String nonProxyHosts) {
    if (!this.nonProxyHosts.equalsIgnoreCase(nonProxyHosts)) {
      this.nonProxyHosts = nonProxyHosts;
      if (routePlanner != null) {
        routePlanner.setNonProxyHosts(nonProxyHosts);
      }
    }
    if (!this.proxyPassword.equalsIgnoreCase(proxyPassword)) {
      this.proxyPassword = proxyPassword;
      credentialsProvider.setCredentials(
          new AuthScope(this.proxyHost, this.proxyPort),
          new UsernamePasswordCredentials(this.proxyUser, this.proxyPassword));
    }
  }

  public SnowflakeProxyRoutePlanner getProxyRoutePlanner() {
    if (routePlanner == null) {
      routePlanner =
          new SnowflakeProxyRoutePlanner(this.proxyHost, this.proxyPort, this.nonProxyHosts);
    }
    return routePlanner;
  }

  public CredentialsProvider getProxyCredentialsProvider() {
    if (!Strings.isNullOrEmpty(this.proxyUser) && !Strings.isNullOrEmpty(this.proxyPassword)) {
      Credentials credentials = new UsernamePasswordCredentials(this.proxyUser, this.proxyPassword);
      AuthScope authScope = new AuthScope(this.proxyHost, this.proxyPort);
      credentialsProvider.setCredentials(authScope, credentials);
      return credentialsProvider;
    }
    return null;
  }
}
