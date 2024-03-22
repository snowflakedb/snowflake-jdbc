package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface ThrowingBiFunction<A, B, R, T extends Throwable> {
  R apply(A a, B b) throws T;
}
