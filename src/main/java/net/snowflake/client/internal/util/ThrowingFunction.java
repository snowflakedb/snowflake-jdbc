package net.snowflake.client.internal.util;

import net.snowflake.client.internal.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface ThrowingFunction<A, R, T extends Throwable> {
  R apply(A a) throws T;
}
