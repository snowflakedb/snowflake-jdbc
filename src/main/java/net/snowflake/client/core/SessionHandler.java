/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.telemetry.Telemetry;

import java.sql.DriverPropertyInfo;
import java.util.List;
import java.util.Properties;

/** Snowflake session implementation */
public interface SessionHandler {
  /**
   * Function that checks if the active session can be closed when the connection is closed. Called
   * by SnowflakeConnectionV1.
   *
   * @return true if it is safe to close this session, false if not
   */
  boolean isSafeToClose();

  public List<DriverPropertyInfo> checkProperties();

  SessionProperties sessionProperties();

  Telemetry getTelemetryClient();

  String getSessionId();

  void open() throws SFException, SnowflakeSQLException;
  /**
   * Close the connection
   *
   * @throws SnowflakeSQLException if failed to close the connection
   * @throws SFException if failed to close the connection
   */
  void close() throws SFException, SnowflakeSQLException;

  Properties getClientInfo();

  String getClientInfo(String name);

  void raiseErrorInSession();

  boolean getAutoCommit();

  void setAutoCommit(boolean autoCommit);

  String getDatabase();

  void setDatabase(String database);

  String getSchema();

  void setSchema(String schema);

  String getRole();

  void setRole(String role);

  String getUser();

  String getUrl();

  String getWarehouse();

  void setWarehouse(String warehouse);

  List<SFException> getSqlWarnings();

  void clearSqlWarnings();
}
