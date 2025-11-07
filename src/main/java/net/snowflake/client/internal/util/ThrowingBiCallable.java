package net.snowflake.client.internal.util;

import net.snowflake.client.internal.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface ThrowingBiCallable<A, B, T extends Throwable> {
  void apply(A a, B b) throws T;
}
