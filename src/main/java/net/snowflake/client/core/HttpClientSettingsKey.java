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

  public HttpClientSettingsKey(
      OCSPMode mode, String host, int port, String nonProxyHosts, String user, String password) {
    this.useProxy = true;
    this.ocspMode = mode != null ? mode : OCSPMode.FAIL_OPEN;
    this.proxyHost = !Strings.isNullOrEmpty(host) ? host.trim() : "";
    this.proxyPort = port;
    this.nonProxyHosts = !Strings.isNullOrEmpty(nonProxyHosts) ? nonProxyHosts.trim() : "";
    this.proxyUser = !Strings.isNullOrEmpty(user) ? user.trim() : "";
    this.proxyPassword = !Strings.isNullOrEmpty(password) ? password.trim() : "";
  }

  public HttpClientSettingsKey(OCSPMode mode) {
    this.useProxy = false;
    this.ocspMode = mode != null ? mode : OCSPMode.FAIL_OPEN;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof HttpClientSettingsKey) {
      HttpClientSettingsKey comparisonKey = (HttpClientSettingsKey) obj;
      if (comparisonKey.ocspMode.getValue() == this.ocspMode.getValue()) {
        if (!comparisonKey.useProxy && !this.useProxy) {
          return true;
        } else if (comparisonKey.proxyHost.equalsIgnoreCase(this.proxyHost)
            && comparisonKey.proxyPort == this.proxyPort
            && comparisonKey.proxyUser.equalsIgnoreCase(this.proxyUser)
            && comparisonKey.proxyPassword.equalsIgnoreCase(this.proxyPassword)) {
          // update nonProxyHost if changed
          if (!this.nonProxyHosts.equalsIgnoreCase(comparisonKey.nonProxyHosts)) {
            comparisonKey.nonProxyHosts = this.nonProxyHosts;
          }
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.ocspMode.getValue()
        + (this.proxyHost + this.proxyPort + this.proxyUser + this.proxyPassword).hashCode();
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
  String getProxyPassword() {
    return this.proxyPassword;
  }

  public String getNonProxyHosts() {
    return this.nonProxyHosts;
  }
}
