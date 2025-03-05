package net.snowflake.client.util;

import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/** Functional interface used to convert data to expected type */
@SnowflakeJdbcInternalApi
@FunctionalInterface
public interface Converter<T> {
  T convert(Object object) throws SFException;
}
