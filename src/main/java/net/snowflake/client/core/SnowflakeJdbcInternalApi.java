package net.snowflake.client.core;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Elements marked with this annotation should be considered as internal API even if they are public
 */
@Target({
  ElementType.PACKAGE,
  ElementType.TYPE,
  ElementType.FIELD,
  ElementType.CONSTRUCTOR,
  ElementType.METHOD
})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SnowflakeJdbcInternalApi {}
