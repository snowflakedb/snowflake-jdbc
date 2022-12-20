package net.snowflake.client.core;

public class HttpClientSettingsKeyBuilder {
  private OCSPMode mode;
  private String host;
  private int port;
  private String nonProxyHosts;
  private String user;
  private String password;
  private String scheme;
  private String userPrefix;
  private boolean useProxy = false;

  public HttpClientSettingsKeyBuilder setMode(OCSPMode mode) {
    this.mode = mode;
    return this;
  }

  public HttpClientSettingsKeyBuilder setHost(String host) {
    this.host = host;
    return this;
  }

  public HttpClientSettingsKeyBuilder setProxy() {
    this.useProxy = true;
    return this;
  }

  public HttpClientSettingsKeyBuilder setPort(int port) {
    this.port = port;
    return this;
  }

  public HttpClientSettingsKeyBuilder setNonProxyHosts(String nonProxyHosts) {
    this.nonProxyHosts = nonProxyHosts;
    return this;
  }

  public HttpClientSettingsKeyBuilder setUser(String user) {
    this.user = user;
    return this;
  }

  public HttpClientSettingsKeyBuilder setPassword(String password) {
    this.password = password;
    return this;
  }

  public HttpClientSettingsKeyBuilder setScheme(String scheme) {
    this.scheme = scheme;
    return this;
  }

  public HttpClientSettingsKeyBuilder setUserPrefix(String userPrefix) {
    this.userPrefix = userPrefix;
    return this;
  }

  public HttpClientSettingsKey createHttpClientSettingsKey() {
    return new HttpClientSettingsKey(
        mode, useProxy, host, port, nonProxyHosts, user, password, scheme, userPrefix);
  }
}
