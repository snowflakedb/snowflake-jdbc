package net.snowflake.client.internal.util;

import net.snowflake.client.internal.core.SFException;

/** Functional interface used to convert data to expected type */
@FunctionalInterface
public interface Converter<T> {
  T convert(Object object) throws SFException;
}
