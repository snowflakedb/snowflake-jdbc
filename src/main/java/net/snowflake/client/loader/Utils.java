/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.loader;

/** Utils class for Loader API */
public class Utils {

  /**
   * Find the root cause of the exception
   *
   * @param e throwable object
   * @return the thowable cause
   */
  public static Throwable getCause(Throwable e) {
    Throwable cause = null;
    Throwable result = e;

    while (null != (cause = result.getCause()) && (result != cause)) {
      result = cause;
    }
    return result;
  }
}
