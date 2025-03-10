package net.snowflake.client.jdbc;

import java.util.ArrayList;
import java.util.List;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.util.ThrowingBiFunction;
import org.apache.http.client.methods.HttpRequestBase;

/**
 * RetryContextManager lets you register logic (as callbacks) that will be re-executed during a
 * retry of a request.
 */
@SnowflakeJdbcInternalApi
public class RetryContextManager {

  // List of retry callbacks that will be executed in the order they were registered.
  private final List<
          ThrowingBiFunction<HttpRequestBase, RetryContext, RetryContext, SnowflakeSQLException>>
      retryCallbacks = new ArrayList<>();

  // A RetryHook flag that can be used by client code to decide when (or if) callbacks should be
  // executed.
  private final RetryHook retryHook;
  private RetryContext retryContext;

  /** Enumeration for different retry hook strategies. */
  public enum RetryHook {
    /** Always execute the registered retry callbacks on every retry. */
    ALWAYS_BEFORE_RETRY,
  }

  /** Default constructor using ALWAYS_BEFORE_RETRY as the default retry hook. */
  public RetryContextManager() {
    this(RetryHook.ALWAYS_BEFORE_RETRY);
  }

  /**
   * Constructor that accepts a specific RetryHook.
   *
   * @param retryHook the retry hook strategy.
   */
  public RetryContextManager(RetryHook retryHook) {
    this.retryHook = retryHook;
    this.retryContext = new RetryContext();
  }

  /**
   * Registers a retry callback that will be executed on each retry.
   *
   * @param callback A RetryCallback encapsulating the logic to be replayed on retry.
   * @return the current instance for fluent chaining.
   */
  public RetryContextManager registerRetryCallback(
      ThrowingBiFunction<HttpRequestBase, RetryContext, RetryContext, SnowflakeSQLException>
          callback) {
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
  protected void executeRetryCallbacks(HttpRequestBase requestToRetry)
      throws SnowflakeSQLException {
    for (ThrowingBiFunction<HttpRequestBase, RetryContext, RetryContext, SnowflakeSQLException>
        callback : retryCallbacks) {
      retryContext = callback.apply(requestToRetry, retryContext);
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

  public RetryContext getRetryContext() {
    return retryContext;
  }
}
