package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface ThrowingTriFunction<A, B, C, R, T extends Throwable> {
  R apply(A a, B b, C c) throws T;
}
