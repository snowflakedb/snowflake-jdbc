package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface ThrowingBiCallable<A, B, T extends Throwable> {
  void apply(A a, B b) throws T;
}
