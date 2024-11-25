/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.google.common.base.Strings;
import java.io.Serializable;

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
  private String proxyProtocol = "http";
  // Adds a suffix to the user agent header in the http requests made by the jdbc driver.
  // More details in SNOW-717606
  private String userAgentSuffix = "";
  private String proxyCrtFile = "";

  private Boolean gzipDisabled = false;

  public HttpClientSettingsKey(
      OCSPMode mode,
      String host,
      int port,
      String nonProxyHosts,
      String user,
      String password,
      String scheme,
      String userAgentSuffix,
      Boolean gzipDisabled) {
    this.useProxy = true;
    this.ocspMode = mode != null ? mode : OCSPMode.FAIL_OPEN;
    this.proxyHost = !Strings.isNullOrEmpty(host) ? host.trim() : "";
    this.proxyPort = port;
    this.nonProxyHosts = !Strings.isNullOrEmpty(nonProxyHosts) ? nonProxyHosts.trim() : "";
    this.proxyUser = !Strings.isNullOrEmpty(user) ? user.trim() : "";
    this.proxyPassword = !Strings.isNullOrEmpty(password) ? password.trim() : "";
    this.proxyProtocol = !Strings.isNullOrEmpty(scheme) ? scheme.trim() : "http";
    this.gzipDisabled = gzipDisabled;
    this.userAgentSuffix = !Strings.isNullOrEmpty(userAgentSuffix) ? userAgentSuffix.trim() : "";
  }


  public HttpClientSettingsKey(
          OCSPMode mode,
          String host,
          int port,
          String nonProxyHosts,
          String user,
          String password,
          String scheme,
          String userAgentSuffix,
          String proxyCrtFile,
          Boolean gzipDisabled) {
    this.useProxy = true;
    this.ocspMode = mode != null ? mode : OCSPMode.FAIL_OPEN;
    this.proxyHost = !Strings.isNullOrEmpty(host) ? host.trim() : "";
    this.proxyPort = port;
    this.proxyCrtFile = proxyCrtFile;
    this.nonProxyHosts = !Strings.isNullOrEmpty(nonProxyHosts) ? nonProxyHosts.trim() : "";
    this.proxyUser = !Strings.isNullOrEmpty(user) ? user.trim() : "";
    this.proxyPassword = !Strings.isNullOrEmpty(password) ? password.trim() : "";
    this.proxyProtocol = !Strings.isNullOrEmpty(scheme) ? scheme.trim() : "http";
    this.gzipDisabled = gzipDisabled;
    this.userAgentSuffix = !Strings.isNullOrEmpty(userAgentSuffix) ? userAgentSuffix.trim() : "";
  }


  public HttpClientSettingsKey(OCSPMode mode) {
    this.useProxy = false;
    this.ocspMode = mode != null ? mode : OCSPMode.FAIL_OPEN;
  }

  HttpClientSettingsKey(OCSPMode mode, String userAgentSuffix, Boolean gzipDisabled) {
    this(mode);
    this.userAgentSuffix = !Strings.isNullOrEmpty(userAgentSuffix) ? userAgentSuffix.trim() : "";
    this.gzipDisabled = gzipDisabled;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof HttpClientSettingsKey) {
      HttpClientSettingsKey comparisonKey = (HttpClientSettingsKey) obj;
      if (comparisonKey.ocspMode.getValue() == this.ocspMode.getValue()) {
        if (comparisonKey.gzipDisabled.equals(this.gzipDisabled)) {
          if (comparisonKey.userAgentSuffix.equalsIgnoreCase(this.userAgentSuffix)) {
            if (!comparisonKey.useProxy && !this.useProxy) {
              return true;
            } else if (comparisonKey.proxyHost.equalsIgnoreCase(this.proxyHost)
                && comparisonKey.proxyPort == this.proxyPort
                && comparisonKey.proxyUser.equalsIgnoreCase(this.proxyUser)
                && comparisonKey.proxyPassword.equalsIgnoreCase(this.proxyPassword)
                && comparisonKey.proxyProtocol.equalsIgnoreCase(this.proxyProtocol)) {
              // update nonProxyHost if changed
              if (!this.nonProxyHosts.equalsIgnoreCase(comparisonKey.nonProxyHosts)) {
                comparisonKey.nonProxyHosts = this.nonProxyHosts;
              }
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.ocspMode.getValue()
        + (this.proxyHost
                + this.proxyPort
                + this.proxyUser
                + this.proxyPassword
                + this.proxyProtocol)
            .hashCode();
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

  public String getUserAgentSuffix() {
    return this.userAgentSuffix;
  }

  /**
   * Be careful of using this! Should only be called when password is later masked.
   *
   * @return proxy password
   */
  @SnowflakeJdbcInternalApi
  public String getProxyPassword() {
    return this.proxyPassword;
  }

  public String getNonProxyHosts() {
    return this.nonProxyHosts;
  }

  /**
   * @deprecated Use {@link #getProxyHttpProtocol()}
   * @return ProxyProtocol
   */
  @Deprecated
  public com.amazonaws.Protocol getProxyProtocol() {
    return this.proxyProtocol.equalsIgnoreCase("https")
        ? com.amazonaws.Protocol.HTTPS
        : com.amazonaws.Protocol.HTTP;
  }

  public HttpProtocol getProxyHttpProtocol() {
    return this.proxyProtocol.equalsIgnoreCase("https") ? HttpProtocol.HTTPS : HttpProtocol.HTTP;
  }

  public Boolean getGzipDisabled() {
    return gzipDisabled;
  }

  public String getProxyCrtFile() {
    return proxyCrtFile;
  }

  @Override
  public String toString() {
    return "HttpClientSettingsKey["
        + "ocspMode="
        + ocspMode
        + ", useProxy="
        + useProxy
        + ", proxyHost='"
        + proxyHost
        + '\''
        + ", proxyPort="
        + proxyPort
        + ", nonProxyHosts='"
        + nonProxyHosts
        + '\''
        + ", proxyUser='"
        + proxyUser
        + '\''
        + ", proxyPassword is "
        + (proxyPassword.isEmpty() ? "not set" : "set")
        + ", proxyProtocol='"
        + proxyProtocol
        + '\''
        + ", userAgentSuffix='"
        + userAgentSuffix
        + '\''
        + ", gzipDisabled="
        + gzipDisabled
        + ']';
  }
}
