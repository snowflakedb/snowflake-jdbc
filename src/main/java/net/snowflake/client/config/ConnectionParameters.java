package net.snowflake.client.config;

import java.util.Properties;

public class ConnectionParameters {
  private String uri;
  private Properties params;

  public ConnectionParameters(String uri, Properties params) {
    this.uri = uri;
    this.params = params;
  }

  public String getUri() {
    return uri;
  }

  public Properties getParams() {
    return params;
  }
}
