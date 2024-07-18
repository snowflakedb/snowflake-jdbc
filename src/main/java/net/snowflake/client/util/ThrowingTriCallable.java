package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface ThrowingTriCallable<A, B, C, T extends Throwable> {
  void apply(A a, B b, C c) throws T;
}
