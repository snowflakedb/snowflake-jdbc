package net.snowflake.client.common.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.Comparator;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * FixedViewColumn annotation is used on fields/columns of a class that is exposed
 *
 * @author jhuang
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@SnowflakeJdbcInternalApi
public @interface FixedViewColumn {
  /**
   * The display name of the column
   *
   * @return displan name
   */
  String name() default "col";

  /**
   * The order in which the fixed view columns should be shown
   *
   * @return ordinal number
   */
  int ordinal() default 0;

  /**
   * The type name
   *
   * @return type name
   */
  String type() default "";

  /**
   * The scale
   *
   * @return the scale
   */
  int scale() default 0;

  /**
   * The name of the parameter used to determine if a column should be visible. If empty (default),
   * ignored.
   */
  String enablingParameterName() default "";

  /** Compares ordinals so {@link FixedViewColumn}s can be sorted */
  public static class OrdinalComparatorForFields implements Comparator<Field> {
    @Override
    public int compare(Field f1, Field f2) {
      FixedViewColumn c1 = f1.getAnnotation(FixedViewColumn.class);
      FixedViewColumn c2 = f2.getAnnotation(FixedViewColumn.class);

      return Integer.compare(c1.ordinal(), c2.ordinal());
    }
  }
}
