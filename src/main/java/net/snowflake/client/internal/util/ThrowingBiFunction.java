package net.snowflake.client.internal.util;

import net.snowflake.client.internal.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface ThrowingBiFunction<A, B, R, T extends Throwable> {
  R apply(A a, B b) throws T;
}
