package net.snowflake.client.loader;

/** Utils class for Loader API */
public class Utils {

  /**
   * Find the root cause of the exception
   *
   * @param e throwable object
   * @return the throwable cause
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
