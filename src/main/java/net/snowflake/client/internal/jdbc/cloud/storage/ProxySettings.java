package net.snowflake.client.internal.jdbc.cloud.storage;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.isNullOrEmpty;

import net.snowflake.client.internal.core.HttpProtocol;

/** Immutable POJO holding extracted proxy parameters, shared across all CSP proxy builders. */
class ProxySettings {
  private final String host;
  private final int port;
  private final HttpProtocol protocol;
  private final String user;
  private final String password;
  private final String nonProxyHosts;

  ProxySettings(
      String host,
      int port,
      HttpProtocol protocol,
      String user,
      String password,
      String nonProxyHosts) {
    this.host = host;
    this.port = port;
    this.protocol = protocol;
    this.user = user;
    this.password = password;
    this.nonProxyHosts = nonProxyHosts;
  }

  String getHost() {
    return host;
  }

  int getPort() {
    return port;
  }

  HttpProtocol getProtocol() {
    return protocol;
  }

  String getUser() {
    return user;
  }

  String getPassword() {
    return password;
  }

  String getNonProxyHosts() {
    return nonProxyHosts;
  }

  boolean hasCredentials() {
    return !isNullOrEmpty(user) && !isNullOrEmpty(password);
  }

  boolean hasNonProxyHosts() {
    return !isNullOrEmpty(nonProxyHosts);
  }
}
