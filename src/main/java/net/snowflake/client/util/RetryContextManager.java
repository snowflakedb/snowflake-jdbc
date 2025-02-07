package net.snowflake.client.util;

import org.apache.http.client.methods.HttpRequestBase;

import java.util.concurrent.Callable;


/**
 * Contains all parameters from RestRequest execute method - to be able to retry it with their change
 */
public class RetryContextManager {
    private Callable<? extends RetryContext> runBeforeRetry;
    private RetryHook retryHook = RetryHook.ALWAYS_BEFORE_RETRY;

    public RetryContextManager(Callable<? extends RetryContext> runBeforeRetry) {
        this(runBeforeRetry, RetryHook.ALWAYS_BEFORE_RETRY);
    }

    public RetryContextManager(Callable<? extends RetryContext> runBeforeRetry, RetryHook retryHook) {
        this.runBeforeRetry = runBeforeRetry;
        this.retryHook = retryHook;
    }

    public static class RetryContext {
        private final HttpRequestBase preparedHttpRequest;

        public RetryContext(HttpRequestBase httpRequest) {

            this.preparedHttpRequest = httpRequest;
        }

        public HttpRequestBase getHttpRequest() {
            return preparedHttpRequest;
        }
    }

    public enum RetryHook {
        ONLY_ON_AUTH_TIMEOUT,
        ALWAYS_BEFORE_RETRY
    }

    //    Builder like
    public void registerRetryCallback(Callable<RetryContext> callback) {
        runBeforeRetry = callback;
    }

    public RetryContext createRetryContext() {
        try {
            return runBeforeRetry.call();
        }
        catch (Exception e) {
//            TODO: correct error type
            throw new RuntimeException(e);
        }
    }

    public RetryHook getRetryHook() {
        return retryHook;
    }
}

