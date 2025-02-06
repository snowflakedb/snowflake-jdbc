package net.snowflake.client.util;

import java.util.concurrent.Callable;

public class RetryContextManager {
    private Callable<RetryContext> runBeforeRetry;

    public static class RetryContext {

    }

    //    Builder like
    public void registerRetryCallback(Callable<RetryContext> callback) {

    }
}

