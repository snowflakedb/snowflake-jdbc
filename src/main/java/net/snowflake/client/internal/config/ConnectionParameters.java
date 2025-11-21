package net.snowflake.client.internal.config;

import java.util.Properties;

public class ConnectionParameters {
  private final String url;
  private final Properties params;

  public ConnectionParameters(String uri, Properties params) {
    this.url = uri;
    this.params = params;
  }

  public String getUrl() {
    return url;
  }

  public Properties getParams() {
    return params;
  }
}
