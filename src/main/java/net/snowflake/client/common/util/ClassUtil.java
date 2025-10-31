package net.snowflake.client.common.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * Class utilities
 *
 * @author jhuang
 */
@SnowflakeJdbcInternalApi
public class ClassUtil {

  /**
   * Retrieving fields list of specified class If recursively is true, retrieving fields from all
   * class hierarchy
   *
   * @param clazz the class for which we want to get declared fields
   * @param recursively whether to search recursively up the super class chain
   * @return list of fields
   */
  public static Field[] getDeclaredFields(Class clazz, boolean recursively) {
    List<Field> fields = new LinkedList<Field>();
    Field[] declaredFields = clazz.getDeclaredFields();
    Collections.addAll(fields, declaredFields);

    Class superClass = clazz.getSuperclass();

    if (superClass != null && recursively) {
      Field[] declaredFieldsOfSuper = getDeclaredFields(superClass, recursively);
      if (declaredFieldsOfSuper.length > 0) {
        Collections.addAll(fields, declaredFieldsOfSuper);
      }
    }

    return fields.toArray(new Field[fields.size()]);
  }

  /**
   * Retrieving fields list of specified class and which are annotated by incoming annotation class
   * If recursively is true, retrieving fields from all class hierarchy
   *
   * @param clazz the class for which we want to get declared fields
   * @param annotationClass - specified annotation class
   * @param recursively whether to search recursively up the super class chain
   * @return list of annotated fields
   */
  public static Field[] getAnnotatedDeclaredFields(
      Class clazz, Class<? extends Annotation> annotationClass, boolean recursively) {
    Field[] allFields = getDeclaredFields(clazz, recursively);
    List<Field> annotatedFields = new LinkedList<Field>();

    for (Field field : allFields) {
      if (field.isAnnotationPresent(annotationClass)) {
        annotatedFields.add(field);
      }
    }

    return annotatedFields.toArray(new Field[annotatedFields.size()]);
  }

  public static List<Object> getFixedViewObjectAsRow(Class clazz, Object object) throws Exception {
    Field[] columns = ClassUtil.getAnnotatedDeclaredFields(clazz, FixedViewColumn.class, true);

    Arrays.sort(columns, new FixedViewColumn.OrdinalComparatorForFields());

    List<Object> row = new ArrayList<Object>(columns.length);

    for (Field column : columns) {
      column.setAccessible(true);

      Object columnObj = column.get(object);

      // convert value to string
      if (columnObj != null) {
        row.add(columnObj.toString());
      } else {
        row.add(columnObj);
      }
    }

    return row;
  }
}
