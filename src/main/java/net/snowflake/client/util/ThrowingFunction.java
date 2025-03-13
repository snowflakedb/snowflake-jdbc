package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface ThrowingFunction<A, R, T extends Throwable> {
  R apply(A a) throws T;
}
