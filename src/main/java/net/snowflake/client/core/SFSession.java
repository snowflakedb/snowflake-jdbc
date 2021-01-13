/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.telemetry.Telemetry;

/** Snowflake session implementation */
public interface SFSession {
  /**
   * Function that checks if the active session can be closed when the connection is closed. If
   * there are active asynchronous queries running, the session should stay open even if the
   * connection closes so that the queries can finish running.
   *
   * @return true if it is safe to close this session, false if not
   */
  boolean isSafeToClose();

  /**
   * @param queryID query ID of the query whose status is being investigated
   * @return enum of type QueryStatus indicating the query's status
   * @throws SQLException
   */
  QueryStatus getQueryStatus(String queryID) throws SQLException;

  void addProperty(SFSessionProperty sfSessionProperty, Object propertyValue) throws SFException;

  /**
   * Add a property If a property is known for connection, add it to connection properties If not,
   * add it as a dynamic session parameters
   *
   * <p>Make sure a property is not added more than once and the number of properties does not
   * exceed limit.
   *
   * @param propertyName property name
   * @param propertyValue property value
   * @throws SFException exception raised from Snowflake components
   */
  void addProperty(String propertyName, Object propertyValue) throws SFException;

  boolean containProperty(String key);

  boolean isStringQuoted();

  boolean isJdbcTreatDecimalAsInt();

  /**
   * Open a new database session
   *
   * @throws SFException this is a runtime exception
   * @throws SnowflakeSQLException exception raised from Snowflake components
   */
  void open() throws SFException, SnowflakeSQLException;

  List<DriverPropertyInfo> checkProperties();

  String getDatabaseVersion();

  int getDatabaseMajorVersion();

  int getDatabaseMinorVersion();

  String getSessionId();

  /**
   * Close the connection
   *
   * @throws SnowflakeSQLException if failed to close the connection
   * @throws SFException if failed to close the connection
   */
  void close() throws SFException, SnowflakeSQLException;

  Properties getClientInfo();

  String getClientInfo(String name);

  void setSFSessionProperty(String propertyName, boolean propertyValue);

  Object getSFSessionProperty(String propertyName);

  boolean isClosed();

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

  Telemetry getTelemetryClient();

  boolean getMetadataRequestUseConnectionCtx();

  boolean getMetadataRequestUseSessionDatabase();

  boolean getPreparedStatementLogging();

  List<SFException> getSqlWarnings();

  void setSfSQLMode(boolean booleanV);

  void clearSqlWarnings();

  void setInjectFileUploadFailure(String fileToFail);

  void setInjectedDelay(int delay);

  boolean isSfSQLMode();

  boolean isResultColumnCaseInsensitive();

  String getServerUrl();

  String getSessionToken();

  String getServiceName();

  String getIdToken();

  SnowflakeType getTimestampMappedType();
}
