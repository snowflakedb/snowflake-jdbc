package net.snowflake.client.jdbc;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/** RetryContext stores information about an ongoing request's retrying process. */
@SnowflakeJdbcInternalApi
public class RetryContext {
  private long elapsedTimeInMillis;
  private long retryTimeoutInMillis;

  public RetryContext() {}

  public RetryContext setElapsedTimeInMillis(long elapsedTimeInMillis) {
    this.elapsedTimeInMillis = elapsedTimeInMillis;
    return this;
  }

  public RetryContext setRetryTimeoutInMillis(long retryTimeoutInMillis) {
    this.retryTimeoutInMillis = retryTimeoutInMillis;
    return this;
  }

  public long getRemainingRetryTimeoutInMillis() {
    return retryTimeoutInMillis - elapsedTimeInMillis;
  }
}
