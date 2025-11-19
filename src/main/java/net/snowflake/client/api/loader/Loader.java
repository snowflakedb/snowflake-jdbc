package net.snowflake.client.api.loader;

/**
 * Bulk loader for Snowflake.
 *
 * <p>This interface extends {@link AutoCloseable}, enabling try-with-resources pattern for
 * automatic resource management:
 *
 * <pre>{@code
 * try (Loader loader = LoaderFactory.createLoader(...)) {
 *     loader.start();
 *     loader.submitRow(data);
 *     loader.finish();
 * } // Connections closed automatically
 * }</pre>
 */
public interface Loader extends AutoCloseable {

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

  /**
   * Close connections that have been provided upon initialization.
   *
   * <p>This method is called automatically when using try-with-resources. It is safe to call this
   * method multiple times.
   *
   * @throws Exception if an error occurs while closing resources
   */
  @Override
  void close() throws Exception;

  // Raised for data conversion errors, if requested
  class DataError extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DataError(String msg) {
      super(msg);
    }

    public DataError(String msg, Throwable ex) {
      super(msg, ex);
    }

    public DataError(Throwable ex) {
      super(ex);
    }
  }

  // Raised for connection and other system errors.
  class ConnectionError extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ConnectionError(String msg) {
      super(msg);
    }

    // SNOW-22336: pass cause to connector
    public ConnectionError(String msg, Throwable ex) {
      super(msg, ex);
    }

    public ConnectionError(Throwable ex) {
      super(ex);
    }
  }

  // get the listener instance used by this loader instance
  LoadResultListener getListener();
}
