package net.snowflake.client.jdbc;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public enum EnvironmentVariables {
  AWS_REGION("AWS_REGION");

  private final String name;

  EnvironmentVariables(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
