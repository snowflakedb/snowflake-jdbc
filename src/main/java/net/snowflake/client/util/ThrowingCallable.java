/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface ThrowingCallable<A, T extends Throwable> {
  A call() throws T;
}
