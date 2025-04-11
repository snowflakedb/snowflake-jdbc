package net.snowflake.client.jdbc;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/** RetryContext stores information about an ongoing request's retrying process. */
@SnowflakeJdbcInternalApi
public class RetryContext {
  static final int SECONDS_TO_MILLIS_FACTOR = 1000;
  private long elapsedTimeInMillis;
  private long retryTimeoutInMillis;
  private long retryCount;
  private long maxRetryCount;
  private boolean noRetry;
  private boolean includeRetryParameters;
  private boolean retryHTTP403;


  public RetryContext() {}

  public RetryContext setElapsedTimeInMillis(long elapsedTimeInMillis) {
    this.elapsedTimeInMillis = elapsedTimeInMillis;
    return this;
  }

  public RetryContext setRetryTimeoutInMillis(long retryTimeoutInMillis) {
    this.retryTimeoutInMillis = retryTimeoutInMillis;
    return this;
  }

  public RetryContext setRetryCount(long retryCount) {
    this.retryCount = retryCount;
    return this;
  }

  public RetryContext setMaxRetryCount(long maxRetryCount) {
    this.maxRetryCount = maxRetryCount;
    return this;
  }

  public RetryContext setRetryTimeoutt(boolean noRetry) {
    this.noRetry = noRetry;
    return this;
  }

  public RetryContext setIncludeRetryParameters(boolean includeRetryParameters) {
    this.includeRetryParameters = includeRetryParameters;
    return this;
  }


  public RetryContext setRetryHTTP403(boolean retryHTTP403) {
    this.retryHTTP403 = retryHTTP403;
    return this;
  }

  private long getRemainingRetryTimeoutInMillis() {
    return retryTimeoutInMillis - elapsedTimeInMillis;
  }

  public long getRemainingRetryTimeoutInSeconds() {
    return (getRemainingRetryTimeoutInMillis()) / SECONDS_TO_MILLIS_FACTOR;
  }

  public long getElapsedTimeInMillis() {
    return elapsedTimeInMillis;
  }

  public long getRetryTimeoutInMillis() {
    return retryTimeoutInMillis;
  }

  public long getRetryCount() {
    return retryCount;
  }

  public long getMaxRetryCount() {
    return maxRetryCount;
  }

  public boolean getNoRetry() {
    return noRetry;
  }

  public boolean getIncludeRetryParameters() {
    return includeRetryParameters;
  }

  public boolean getRetryHTTP403() {
    return retryHTTP403;
  }
}
