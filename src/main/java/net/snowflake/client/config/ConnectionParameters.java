package net.snowflake.client.config;

import java.util.Properties;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
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
