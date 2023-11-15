package net.snowflake.client.util;

public class Predicates {
  public static void notNull(Object o, String msg) {
    if (o == null) {
      throw new NullPointerException(msg);
    }
  }
}
