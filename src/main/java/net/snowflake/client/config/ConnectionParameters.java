/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.config;

import java.util.Properties;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
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
