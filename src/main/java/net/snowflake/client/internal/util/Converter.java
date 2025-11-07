package net.snowflake.client.internal.util;

import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SnowflakeJdbcInternalApi;

/** Functional interface used to convert data to expected type */
@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface Converter<T> {
  T convert(Object object) throws SFException;
}
