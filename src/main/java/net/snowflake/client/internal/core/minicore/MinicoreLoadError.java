package net.snowflake.client.internal.core.minicore;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public enum MinicoreLoadError {
  DISABLED("Minicore is disabled with SNOWFLAKE_DISABLE_MINICORE env variable"),
  FAILED_TO_LOAD("Failed to load binary"),
  STILL_LOADING("Minicore is still loading");

  private final String message;

  MinicoreLoadError(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return message;
  }
}
