/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.loader;

import java.io.File;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

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

}
