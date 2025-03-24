package net.snowflake.client.log;

/**
 * Interface used by JDBC driver to log information
 *
 * <p>Five levels are included in this interface, from high to low: ERROR WARN INFO DEBUG TRACE
 */
public interface SFLogger {
  /**
   * Is debug level enabled?
   *
   * @return true if the trace level is DEBUG
   */
  boolean isDebugEnabled();

  /**
   * Is error level enabled?
   *
   * @return true if the trace level is ERROR
   */
  boolean isErrorEnabled();

  /**
   * Is info level enabled?
   *
   * @return true if the trace level is INFO
   */
  boolean isInfoEnabled();

  /**
   * Is trace level enabled?
   *
   * @return true if the trace level is TRACE
   */
  boolean isTraceEnabled();

  /**
   * Is warn level enabled?
   *
   * @return true if the trace level is WARN
   */
  boolean isWarnEnabled();

  void debug(String msg, boolean isMasked);

  void debugNoMask(String msg);

  /**
   * Logs message at DEBUG level.
   *
   * @param msg Message or message format
   * @param arguments objects that supply value to placeholders in the message format. Expensive
   *     operations that supply these values can be specified using lambdas implementing {@link
   *     ArgSupplier} so that they are run only if the message is going to be logged. E.g., {@code
   *     Logger.debug("Value: {}", (ArgSupplier) () -> expensiveOperation());}
   */
  void debug(String msg, Object... arguments);

  void debug(String msg, Throwable t);

  void error(String msg, boolean isMasked);

  /**
   * Logs message at ERROR level.
   *
   * @param msg Message or message format
   * @param arguments objects that supply value to placeholders in the message format. Expensive
   *     operations that supply these values can be specified using lambdas implementing {@link
   *     ArgSupplier} so that they are run only if the message is going to be logged. E.g., {@code
   *     Logger.warn("Value: {}", (ArgSupplier) () -> expensiveOperation());}
   */
  void error(String msg, Object... arguments);

  void error(String msg, Throwable t);

  void info(String msg, boolean isMasked);

  /**
   * Logs message at INFO level.
   *
   * @param msg Message or message format
   * @param arguments objects that supply value to placeholders in the message format. Expensive
   *     operations that supply these values can be specified using lambdas implementing {@link
   *     ArgSupplier} so that they are run only if the message is going to be logged. E.g., {@code
   *     Logger.info("Value: {}", (ArgSupplier) () -> expensiveOperation());}
   */
  void info(String msg, Object... arguments);

  void info(String msg, Throwable t);

  void trace(String msg, boolean isMasked);

  /**
   * Logs message at TRACE level.
   *
   * @param msg Message or message format
   * @param arguments objects that supply value to placeholders in the message format. Expensive
   *     operations that supply these values can be specified using lambdas implementing {@link
   *     ArgSupplier} so that they are run only if the message is going to be logged. E.g., {@code
   *     Logger.trace("Value: {}", (ArgSupplier) () -> expensiveOperation());}
   */
  void trace(String msg, Object... arguments);

  void trace(String msg, Throwable t);

  void warn(String msg, boolean isMasked);

  /**
   * Logs message at WARN level.
   *
   * @param msg Message or message format
   * @param arguments objects that supply value to placeholders in the message format. Expensive
   *     operations that supply these values can be specified using lambdas implementing {@link
   *     ArgSupplier} so that they are run only if the message is going to be logged. E.g., {@code
   *     Logger.warn("Value: {}", (ArgSupplier) () -> expensiveOperation());}
   */
  void warn(String msg, Object... arguments);

  void warn(String msg, Throwable t);
}
