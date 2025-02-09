package net.snowflake.client.util;

import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * RetryContextManager lets you register logic (as callbacks) that will be re-executed during a retry.*
 */
public class RetryContextManager {

    // List of retry callbacks that will be executed in the order they were registered.
    private final List<RetryCallback<HttpRequestBase>> retryCallbacks = new ArrayList<>();

    // A RetryHook flag that can be used by client code to decide when (or if) callbacks should be executed.
    private final RetryHook retryHook;

    /**
     * Enumeration for different retry hook strategies.
     */
    public enum RetryHook {
        /**
         * Always execute the registered retry callbacks on every retry.
         */
        ALWAYS_BEFORE_RETRY,
        /**
         * Only execute the callbacks on specific conditions (e.g. authentication timeouts).
         */
        ONLY_ON_AUTH_TIMEOUT
    }

    /**
     * A functional interface for retry callbacks that operate on an object of type T
     * and may throw a SnowflakeSQLException.
     *
     * @param <T> the type of the input to the callback.
     */
    @FunctionalInterface
    public interface RetryCallback<T> {
        /**
         * Performs this operation on the given argument.
         *
         * @param t the input argument.
         * @throws SnowflakeSQLException if an error occurs during the callback execution.
         */
        void accept(T t) throws SnowflakeSQLException;
    }

    /**
     * Default constructor using ALWAYS_BEFORE_RETRY as the default retry hook.
     */
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
    }

    /**
     * Registers a retry callback that will be executed on each retry.
     *
     * @param callback A RetryCallback<HttpRequestBase> encapsulating the logic to be replayed on retry.
     * @return the current instance for fluent chaining.
     */
    public RetryContextManager registerRetryCallback(RetryCallback<HttpRequestBase> callback) {
        retryCallbacks.add(callback);
        return this;
    }

    /**
     * Executes all registered retry callbacks in the order they were added.
     * This method should be called from within your retry-handling logic (e.g. in RestRequest.execute)
     * before reattempting the operation.
     */
    public void executeRetryCallbacks(HttpRequestBase requestToRetry) throws SnowflakeSQLException {
        for (RetryCallback<HttpRequestBase> callback : retryCallbacks) {
            callback.accept(requestToRetry);
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
