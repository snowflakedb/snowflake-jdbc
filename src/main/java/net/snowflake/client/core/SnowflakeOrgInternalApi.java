package net.snowflake.client.core;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Elements marked with this annotation should be considered as internal API and are public API only
 * for Snowflake internal tools. Starting from version 4.x there will be no guarantees that
 * annotated elements will maintain backward compatibility.
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
public @interface SnowflakeOrgInternalApi {}
