/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.sql.DriverPropertyInfo;
import java.util.List;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.telemetry.Telemetry;

/** Snowflake session implementation */
public interface SFSessionInterface {
  /**
   * Function that checks if the active session can be closed when the connection is closed. Called
   * by SnowflakeConnectionV1.
   */
  boolean isSafeToClose();

  /**
   * Validates the connection properties used by this session, and returns a list of missing
   * properties.
   */
  List<DriverPropertyInfo> checkProperties();

  /**
   * Gets the SessionProperties for this session, which includes connection properties, custom
   * properties, and session properties returned from the server.
   */
  SessionProperties getSessionProperties();

  /**
   * Opens the connection with this session
   *
   * @throws SFException if failed to open the connection
   * @throws SnowflakeSQLException if failed to open the connection
   */
  void open() throws SFException, SnowflakeSQLException;

  /**
   * Close the connection
   *
   * @throws SnowflakeSQLException if failed to close the connection
   * @throws SFException if failed to close the connection
   */
  void close() throws SFException, SnowflakeSQLException;

  /**
   * Raise an error within the current session. By default, this may log an incident with Snowflake.
   *
   * @param exc The throwable exception
   * @param jobId jobId that failed
   * @param requestId requestId that failed
   */
  void raiseError(Throwable exc, String jobId, String requestId);

  /**
   * JDBC API. Returns a list of warnings generated since starting this session, or the last time it
   * was cleared.
   */
  List<SFException> getSqlWarnings();

  /**
   * JDBC API. Clears the list of warnings generated since the start of the session, or the last
   * time it was cleared.
   */
  void clearSqlWarnings();

  /** Returns the telemetry client, if supported, by this session. */
  Telemetry getTelemetryClient();

  /** Whether to enable telemetry when using this session implementation. */
  boolean isTelemetryEnabled();
}
