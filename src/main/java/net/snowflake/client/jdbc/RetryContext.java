package net.snowflake.client.jdbc;

import java.util.ArrayList;
import java.util.List;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.util.ThrowingFunction;
import org.apache.http.client.methods.HttpRequestBase;

/**
 * RetryContext lets you register logic (as callbacks) that will be re-executed during a
 * retry.
 */
@SnowflakeJdbcInternalApi
public class RetryContext {

  // List of retry callbacks that will be executed in the order they were registered.
  private final List<ThrowingFunction<HttpRequestBase, Void, SnowflakeSQLException>> retryCallbacks = new ArrayList<>();

  // A RetryHook flag that can be used by client code to decide when (or if) callbacks should be
  // executed.
  private final RetryHook retryHook;

  /** Enumeration for different retry hook strategies. */
  public enum RetryHook {
    /** Always execute the registered retry callbacks on every retry. */
    ALWAYS_BEFORE_RETRY,
    /** Only execute the callbacks on specific conditions (e.g. authentication timeouts). */
    ONLY_ON_AUTH_TIMEOUT
  }

  /** Default constructor using ALWAYS_BEFORE_RETRY as the default retry hook. */
  public RetryContext() {
    this(RetryHook.ALWAYS_BEFORE_RETRY);
  }

  /**
   * Constructor that accepts a specific RetryHook.
   *
   * @param retryHook the retry hook strategy.
   */
  public RetryContext(RetryHook retryHook) {
    this.retryHook = retryHook;
  }

  /**
   * Registers a retry callback that will be executed on each retry.
   *
   * @param callback A RetryCallback encapsulating the logic to be replayed on retry.
   * @return the current instance for fluent chaining.
   */
  public RetryContext registerRetryCallback(ThrowingFunction<HttpRequestBase, Void, SnowflakeSQLException> callback) {
    retryCallbacks.add(callback);
    return this;
  }

  /**
   * Executes all registered retry callbacks in the order they were added, before reattempting the
   * operation.
   *
   * @param requestToRetry the HTTP request to retry.
   * @throws SnowflakeSQLException if an error occurs during callback execution.
   */
  public void executeRetryCallbacks(HttpRequestBase requestToRetry) throws SnowflakeSQLException {
    for (ThrowingFunction<HttpRequestBase, Void, SnowflakeSQLException> callback : retryCallbacks) {
      callback.apply(requestToRetry);
    }
  }

  /**
   * Returns the configured RetryHook.
   *
   * @return the retry hook.
   */
  public RetryHook getRetryHook() {
    return retryHook;
  }
}
