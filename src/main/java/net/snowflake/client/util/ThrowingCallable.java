package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface ThrowingCallable<A, T extends Throwable> {
  A call() throws T;
}
