package net.snowflake.client.core;

public enum HttpProtocol {
  HTTP("http"),

  HTTPS("https");

  private final String scheme;

  HttpProtocol(String scheme) {
    this.scheme = scheme;
  }

  public String getScheme() {
    return scheme;
  }
}
