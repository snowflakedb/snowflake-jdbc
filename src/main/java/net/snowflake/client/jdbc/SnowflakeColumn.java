package net.snowflake.client.jdbc;

import static java.lang.annotation.ElementType.FIELD;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface SnowflakeColumn {

  /**
   * (Optional) The name for a column in database,
   *
   * @return The default value is empty string. Provided name can override SqlData field name.
   */
  String name() default "";

  /**
   * (Optional) The snowflake type for a column
   *
   * @return The default value is empty string Provided type can override default type.
   */
  String type() default "";

  /**
   * (Optional) The snowflake nullable flag for a column
   *
   * @return The default value is true Provided value can override default nullable value.
   */
  boolean nullable() default true;

  /**
   * (Optional) The length for a column of SQL type {@code varchar} or {@code binary}, or of similar
   * database-native type.
   *
   * <p>Applies only to columns of exact varchar and binary type.
   *
   * @return The default value {@code -1} indicates that a provider-determined length should be
   *     inferred.
   */
  int length() default -1;
  /**
   * (Optional) The length for a column of SQL type {@code binary}, or of similar database-native
   * type.
   *
   * <p>Applies only to columns of exact varchar and binary type.
   *
   * @return The default value {@code -1} indicates that a provider-determined byteLength should be
   *     inferred.
   */
  int byteLength() default -1;

  /**
   * (Optional) The precision for a column of SQL type {@code decimal} or {@code numeric}, or of
   * similar database-native type.
   *
   * <p>Applies only to columns of exact numeric type.
   *
   * @return The default value {@code -1} indicates that a provider-determined precision should be
   *     inferred.
   */
  int precision() default -1;

  /**
   * (Optional) The scale for a column of SQL type {@code decimal} or {@code numeric}, or of similar
   * database-native type.
   *
   * <p>Applies only to columns of exact numeric type.
   *
   * @return The default value {@code 0} indicates that a provider-determined scale should be
   *     inferred.
   */
  int scale() default -1;
}
