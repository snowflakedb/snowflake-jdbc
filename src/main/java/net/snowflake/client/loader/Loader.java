package net.snowflake.client.loader;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.File;

/** Bulk loader for Snowflake */
public interface Loader {

  // Temporary directory used for data cache
  String tmpdir = systemGetProperty("java.io.tmpdir");

  String BASE =
      tmpdir
          + (!(tmpdir.endsWith("/") || tmpdir.endsWith("\\")) ? File.separatorChar : "")
          + "snowflake"
          + File.separatorChar
          + "stage";

  // Configuration, see LoaderProperty
  void setProperty(LoaderProperty property, Object value);

  // Callback API
  void setListener(LoadResultListener listener);

  /** Initiates loading threads. Executes "before" statement */
  void start();

  /**
   * Pass row data
   *
   * @param data, must match shape of the table (requested columns, in the order provided)
   */
  void submitRow(Object[] data);

  /**
   * If operation is changed, previous data is committed
   *
   * @param op operation will be reset
   */
  void resetOperation(Operation op);

  /**
   * Rollback uncommitted changes. If no transaction was initialized, indeterminate fraction of rows
   * could have been committed.
   */
  void rollback();

  /**
   * Finishes processing and commits or rolls back. Will throw the exception that was the cause of
   * an abort
   *
   * @throws Exception if that was the cause of an abort
   */
  void finish() throws Exception;

  /** Close connections that have been provided upon initialization */
  void close();

  // Raised for data conversion errors, if requested
  class DataError extends RuntimeException {
    private static final long serialVersionUID = 1L;

    DataError(String msg) {
      super(msg);
    }

    DataError(String msg, Throwable ex) {
      super(msg, ex);
    }

    DataError(Throwable ex) {
      super(ex);
    }
  }

  // Raised for connection and other system errors.
  class ConnectionError extends RuntimeException {
    private static final long serialVersionUID = 1L;

    ConnectionError(String msg) {
      super(msg);
    }

    // SNOW-22336: pass cause to connector
    ConnectionError(String msg, Throwable ex) {
      super(msg, ex);
    }

    ConnectionError(Throwable ex) {
      super(ex);
    }
  }

  // get the listener instance used by this loader instance
  LoadResultListener getListener();
}
