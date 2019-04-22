/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.google.common.base.Strings;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.LoginInfoDTO;
import net.snowflake.common.core.SqlState;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import static net.snowflake.client.core.SessionUtil.JVM_PARAMS_TO_PARAMS;
import static net.snowflake.client.jdbc.ErrorCode.FEATURE_UNSUPPORTED;

/**
 * Snowflake connection implementation
 */
public class SnowflakeConnectionV1 implements Connection
{

  static private final
  SFLogger logger = SFLoggerFactory.getLogger(SnowflakeConnectionV1.class);

  private static final String JDBC_PROTOCOL_PREFIX = "jdbc:snowflake";

  private static final String NATIVE_PROTOCOL = "http";

  private static final String SSL_NATIVE_PROTOCOL = "https";

  private boolean isClosed;

  private SQLWarning sqlWarnings = null;

  // Injected delay for the purpose of connection timeout testing
  // Any statement execution will sleep for the specified number of milliseconds
  private AtomicInteger _injectedDelay = new AtomicInteger(0);

  private String databaseVersion;
  private int databaseMajorVersion;
  private int databaseMinorVersion;


  /**
   * Amount of milliseconds a user is willing to tolerate for network related
   * issues (e.g. HTTP 503/504) or database transient issues (e.g. GS
   * not responding)
   * <p>
   * A value of 0 means no timeout
   * <p>
   * Default: 300 seconds
   */
  private int networkTimeoutInMilli = 0; // in milliseconds

  private Properties clientInfo = new Properties();

  // TODO this should be set to Connection.TRANSACTION_READ_COMMITTED
  // TODO There may not be many implications here since the call to
  // TODO setTransactionIsolation doesn't do anything.
  private int transactionIsolation = Connection.TRANSACTION_NONE;

  private SFSession sfSession;

  /**
   * A connection will establish a session token from snowflake
   *
   * @param url  server url used to create snowflake connection
   * @param info properties about the snowflake connection
   * @throws SQLException if failed to create a snowflake connection
   *                      i.e. username or password not specified
   */
  public SnowflakeConnectionV1(String url,
                               Properties info)
  throws SQLException
  {
    logger.debug("Trying to establish session, JDBC driver version: {}",
                 SnowflakeDriver.implementVersion);
    TelemetryService.getInstance().updateContext(url, info);
    // open connection to GS
    sfSession = new SFSession();

    try
    {
      // pass the parameters to sfSession
      initSessionProperties(url, info);

      sfSession.open();

      // SNOW-18963: return database version
      databaseVersion = sfSession.getDatabaseVersion();
      databaseMajorVersion = sfSession.getDatabaseMajorVersion();
      databaseMinorVersion = sfSession.getDatabaseMinorVersion();
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(), ex.getSqlState(),
                                      ex.getVendorCode(), ex.getParams());
    }

    appendWarnings(sfSession.getSqlWarnings());

    isClosed = false;
  }

  private void raiseSQLExceptionIfConnectionIsClosed() throws SQLException
  {
    if (isClosed)
    {
      throw new SnowflakeSQLException(ErrorCode.CONNECTION_CLOSED);
    }
  }

  /**
   * Processes parameters given in the connection string. This extracts
   * accountName, databaseName, schemaName from
   * the URL if it is specified there, where the URL is of the form:
   * <p>
   * jdbc:snowflake://host:port/?user=v&password=v&account=v&
   * db=v&schema=v&ssl=v&[passcode=v|passcodeInPassword=on]
   *
   * @param url  a URL string
   * @param info additional properties
   */
  static Map<String, Object> mergeProperties(String url, Properties info)
  {
    final Map<String, Object> properties = new HashMap<>();

    // first deal with url
    int queryParamsIndex = url.indexOf("?");

    String serverUrl = queryParamsIndex > 0 ? url.substring(0, queryParamsIndex)
                                            : url;
    String queryParams = queryParamsIndex > 0 ? url.substring(queryParamsIndex + 1)
                                              : null;

    if (queryParams != null && !queryParams.isEmpty())
    {
      String[] entries = queryParams.split("&");

      for (String entry : entries)
      {
        int sep = entry.indexOf("=");
        if (sep > 0)
        {
          String key = entry.substring(0, sep).toUpperCase();
          properties.put(key, entry.substring(sep + 1));
        }
      }
    }

    for (Map.Entry<Object, Object> entry : info.entrySet())
    {
      properties.put(entry.getKey().toString().toUpperCase(), entry.getValue());
    }

    boolean sslOn = getBooleanTrueByDefault(properties.get("SSL"));

    serverUrl = serverUrl.replace(JDBC_PROTOCOL_PREFIX,
                                  sslOn ? SSL_NATIVE_PROTOCOL : NATIVE_PROTOCOL);

    properties.put("SERVERURL", serverUrl);
    properties.remove("SSL");

    // extracting ACCOUNT from URL if not set in the parameter
    if (properties.get("ACCOUNT") == null &&
        serverUrl.indexOf(".") > 0 &&
        serverUrl.indexOf("://") > 0)
    {
      String accountName = serverUrl.substring(serverUrl.indexOf("://") + 3,
                                               serverUrl.indexOf("."));

      logger.debug("set account name to {}", accountName);
      properties.put("ACCOUNT", accountName);
    }

    return properties;
  }

  private void initSessionProperties(String url, Properties info) throws SFException
  {
    Map<String, Object> properties = mergeProperties(url, info);

    for (Map.Entry<String, Object> property : properties.entrySet())
    {
      sfSession.addProperty(property.getKey(), property.getValue());
    }

    // populate app id and version
    sfSession.addProperty(SFSessionProperty.APP_ID, LoginInfoDTO.SF_JDBC_APP_ID);
    sfSession.addProperty(SFSessionProperty.APP_VERSION,
                          SnowflakeDriver.implementVersion);

    // Set the corresponding session parameters to the JVM properties
    for (Map.Entry<String, String> entry : JVM_PARAMS_TO_PARAMS.entrySet())
    {
      String value = System.getProperty(entry.getKey());
      if (value != null && !sfSession.containProperty(entry.getValue()))
      {
        sfSession.addProperty(entry.getValue(), value);
      }
    }
  }

  private static boolean getBooleanTrueByDefault(Object value)
  {
    if (value instanceof String)
    {
      final String value0 = (String) value;
      return !("off".equalsIgnoreCase(value0) || "false".equalsIgnoreCase(value0));
    }
    else if (value instanceof Boolean)
    {
      return (boolean) value;
    }
    return true;
  }

  /**
   * Execute a statement where the result isn't needed, and the statement is
   * closed before this method returns
   *
   * @param stmtText text of the statement
   * @throws SQLException exception thrown it the statement fails to execute
   */
  private void executeImmediate(String stmtText) throws SQLException
  {
    // execute the statement and auto-close it as well
    try (final Statement statement = this.createStatement())
    {
      statement.execute(stmtText);
    }
  }

  /**
   * Create a statement
   *
   * @return statement statement object
   * @throws SQLException if failed to create a snowflake statement
   */
  @Override
  public Statement createStatement() throws SQLException
  {
    raiseSQLExceptionIfConnectionIsClosed();
    return createStatement(
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);
  }

  /**
   * Close the connection
   *
   * @throws SQLException failed to close the connection
   */
  @Override
  public void close() throws SQLException
  {
    logger.debug(" public void close()");

    if (isClosed)
    {
      // No exception is raised even if the connection is closed.
      return;
    }

    isClosed = true;
    try
    {
      if (sfSession != null)
      {
        sfSession.close();
        sfSession = null;
      }
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    logger.debug(" public boolean isClosed()");

    return isClosed;
  }

  /**
   * Return the database metadata
   *
   * @return Database metadata
   * @throws SQLException if any database error occurs
   */
  @Override
  public DatabaseMetaData getMetaData() throws SQLException
  {
    logger.debug(
        " public DatabaseMetaData getMetaData()");
    raiseSQLExceptionIfConnectionIsClosed();
    return new SnowflakeDatabaseMetaData(this);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException
  {
    logger.debug(
        " public CallableStatement prepareCall(String sql)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency)
  throws SQLException
  {
    logger.debug(
        " public CallableStatement prepareCall(String sql," +
        " int resultSetType,int resultSetConcurrency");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency,
                                       int resultSetHoldability)
  throws SQLException
  {
    logger.debug(
        " public CallableStatement prepareCall(String sql, int "
        + "resultSetType,");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException
  {
    logger.debug("public String nativeSQL(String sql)");
    raiseSQLExceptionIfConnectionIsClosed();
    return sql;
  }

  @Override
  public void setAutoCommit(boolean isAutoCommit) throws SQLException
  {
    logger.debug("void setAutoCommit(boolean isAutoCommit)");
    this.executeImmediate(
        "alter session /* JDBC:SnowflakeConnectionV1.setAutoCommit*/ set autocommit=" +
        isAutoCommit);
  }

  @Override
  public boolean getAutoCommit() throws SQLException
  {
    logger.debug("boolean getAutoCommit()");
    raiseSQLExceptionIfConnectionIsClosed();
    return sfSession.getAutoCommit();
  }

  @Override
  public void commit() throws SQLException
  {
    logger.debug("void commit()");
    this.executeImmediate("commit");
  }

  @Override
  public void rollback() throws SQLException
  {
    logger.debug("void rollback()");
    this.executeImmediate("rollback");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException
  {
    logger.debug(
        "void rollback(Savepoint savepoint)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException
  {
    logger.debug(
        "void setReadOnly(boolean readOnly)");
    raiseSQLExceptionIfConnectionIsClosed();
    if (readOnly)
    {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException
  {
    logger.debug("boolean isReadOnly()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException
  {
    logger.debug(
        "void setCatalog(String catalog)");

    // switch db by running "use db"
    this.executeImmediate("use database \"" + catalog + "\"");
  }

  @Override
  public String getCatalog() throws SQLException
  {
    raiseSQLExceptionIfConnectionIsClosed();
    return sfSession.getDatabase();
  }

  /**
   * Sets the transaction isolation level.
   *
   * @param level transaction level: TRANSACTION_NONE or TRANSACTION_READ_COMMITTED
   * @throws SQLException if any SQL error occurs
   */
  @Override
  public void setTransactionIsolation(int level) throws SQLException
  {
    logger.debug(
        "void setTransactionIsolation(int level), level = {}", level);
    raiseSQLExceptionIfConnectionIsClosed();
    if (level == Connection.TRANSACTION_NONE
        || level == Connection.TRANSACTION_READ_COMMITTED)
    {
      this.transactionIsolation = level;
    }
    else
    {
      throw new SQLFeatureNotSupportedException(
          "Transaction Isolation " + level + " not supported.",
          FEATURE_UNSUPPORTED.getSqlState(),
          FEATURE_UNSUPPORTED.getMessageCode());
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException
  {
    logger.debug("int getTransactionIsolation()");
    raiseSQLExceptionIfConnectionIsClosed();
    return this.transactionIsolation;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    logger.debug("SQLWarning getWarnings()");
    raiseSQLExceptionIfConnectionIsClosed();
    return sqlWarnings;
  }

  @Override
  public void clearWarnings() throws SQLException
  {
    logger.debug("void clearWarnings()");
    raiseSQLExceptionIfConnectionIsClosed();
    sfSession.clearSqlWarnings();
    sqlWarnings = null;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
  throws SQLException
  {
    logger.debug(
        "Statement createStatement(int resultSetType, "
        + "int resultSetConcurrency)");

    return createStatement(
        resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                   int resultSetHoldability)
  throws SQLException
  {
    logger.debug(
        "Statement createStatement(int resultSetType, "
        + "int resultSetConcurrency, int resultSetHoldability");

    return new SnowflakeStatementV1(
        this, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException
  {
    logger.debug("PreparedStatement prepareStatement(String sql)");
    raiseSQLExceptionIfConnectionIsClosed();
    return prepareStatement(sql, false);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
  throws SQLException
  {
    logger.debug(
        "PreparedStatement prepareStatement(String sql, "
        + "int autoGeneratedKeys)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
  throws SQLException
  {
    logger.debug(
        "PreparedStatement prepareStatement(String sql, "
        + "int[] columnIndexes)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
  throws SQLException
  {
    logger.debug(
        "PreparedStatement prepareStatement(String sql, "
        + "String[] columnNames)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql,
      int resultSetType,
      int resultSetConcurrency)
  throws SQLException
  {
    logger.debug(
        "PreparedStatement prepareStatement(String sql, "
        + "int resultSetType,");

    return prepareStatement(sql, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability)
  throws SQLException
  {
    logger.debug(
        "PreparedStatement prepareStatement(String sql, "
        + "int resultSetType,");

    return new SnowflakePreparedStatementV1(
        this, sql,
        false,
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability);
  }

  public PreparedStatement prepareStatement(String sql, boolean skipParsing)
  throws SQLException
  {
    logger.debug(
        "PreparedStatement prepareStatement(String sql, boolean skipParsing)");
    raiseSQLExceptionIfConnectionIsClosed();
    return new SnowflakePreparedStatementV1(
        this,
        sql,
        skipParsing,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException
  {
    raiseSQLExceptionIfConnectionIsClosed();
    return Collections.emptyMap(); // nop
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException
  {
    raiseSQLExceptionIfConnectionIsClosed();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT; // nop
  }

  @Override
  public Savepoint setSavepoint() throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob createBlob() throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob createClob() throws SQLException
  {
    raiseSQLExceptionIfConnectionIsClosed();
    return new SnowflakeClob();
  }

  @Override
  public NClob createNClob() throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException
  {
    // TODO: run query here or ping
    if (timeout < 0)
    {
      throw new SQLException("timeout is less than 0");
    }
    return !isClosed; // no exception is raised
  }

  @Override
  public void setClientInfo(Properties properties)
  throws SQLClientInfoException
  {
    logger.debug(
        "void setClientInfo(Properties properties)");

    if (isClosed)
    {
      throw new SQLClientInfoException();
    }

    // make a copy, don't point to the properties directly since we don't
    // own it.
    this.clientInfo.clear();
    this.clientInfo.putAll(properties);

    // sfSession must not be null if the connection is not closed.
    sfSession.setClientInfo(properties);
  }

  @Override
  public void setClientInfo(String name, String value)
  throws SQLClientInfoException
  {
    logger.debug("void setClientInfo(String name, String value)");

    if (isClosed)
    {
      throw new SQLClientInfoException();
    }

    this.clientInfo.setProperty(name, value);

    // sfSession must not be null if the connection is not closed.
    sfSession.setClientInfo(name, value);
  }

  @Override
  public Properties getClientInfo() throws SQLException
  {
    logger.debug("Properties getClientInfo()");
    raiseSQLExceptionIfConnectionIsClosed();
    // sfSession must not be null if the connection is not closed.
    return sfSession.getClientInfo();
  }

  @Override
  public String getClientInfo(String name) throws SQLException
  {
    logger.debug("String getClientInfo(String name)");

    raiseSQLExceptionIfConnectionIsClosed();
    // sfSession must not be null if the connection is not closed.
    return sfSession.getClientInfo(name);
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements)
  throws SQLException
  {
    logger.debug(
        "Array createArrayOf(String typeName, Object[] "
        + "elements)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes)
  throws SQLException
  {
    logger.debug(
        "Struct createStruct(String typeName, Object[] "
        + "attributes)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setSchema(String schema) throws SQLException
  {
    logger.debug(
        "void setSchema(String schema)");

    String databaseName = getCatalog();

    // switch schema by running "use db.schema"
    if (databaseName == null)
    {
      this.executeImmediate("use schema \"" + schema + "\"");
    }
    else
    {
      this.executeImmediate("use schema \"" + databaseName + "\".\"" + schema + "\"");
    }
  }

  @Override
  public String getSchema() throws SQLException
  {
    raiseSQLExceptionIfConnectionIsClosed();
    return sfSession.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException
  {
    logger.debug(
        "void abort(Executor executor)");

    close();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds)
  throws SQLException
  {
    logger.debug(
        "void setNetworkTimeout(Executor executor, int "
        + "milliseconds)");
    raiseSQLExceptionIfConnectionIsClosed();

    networkTimeoutInMilli = milliseconds;
  }

  @Override
  public int getNetworkTimeout() throws SQLException
  {
    logger.debug(
        "int getNetworkTimeout()");
    raiseSQLExceptionIfConnectionIsClosed();
    return networkTimeoutInMilli;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    logger.debug("boolean isWrapperFor(Class<?> iface)");

    return iface.isInstance(this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    logger.debug("<T> T unwrap(Class<T> iface)");

    if (!iface.isInstance(this))
    {
      throw new SQLException(
          this.getClass().getName() + " not unwrappable from " + iface.getName());
    }
    return (T) this;
  }

  int getDatabaseMajorVersion()
  {
    return databaseMajorVersion;
  }

  int getDatabaseMinorVersion()
  {
    return databaseMinorVersion;
  }

  String getDatabaseVersion()
  {
    return databaseVersion;
  }

  /**
   * Method to put data from a stream at a stage location. The data will be
   * uploaded as one file. No splitting is done in this method.
   * <p>
   * Stream size must match the total size of data in the input stream unless
   * compressData parameter is set to true.
   * <p>
   * caller is responsible for passing the correct size for the data in the
   * stream and releasing the inputStream after the method is called.
   * <p>
   * Note this method is deprecated since streamSize is not required now. Keep
   * the function signature for backward compatibility
   *
   * @param stageName    stage name: e.g. ~ or table name or stage name
   * @param destPrefix   path prefix under which the data should be uploaded on the stage
   * @param inputStream  input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @param streamSize   data size in the stream
   * @throws SQLException failed to put data from a stream at stage
   */
  @Deprecated
  public void uploadStream(String stageName,
                           String destPrefix,
                           InputStream inputStream,
                           String destFileName,
                           long streamSize)
  throws SQLException
  {
    uploadStreamInternal(stageName, destPrefix, inputStream,
                         destFileName, false);
  }

  /**
   * Method to compress data from a stream and upload it at a stage location.
   * The data will be uploaded as one file. No splitting is done in this method.
   * <p>
   * caller is responsible for releasing the inputStream after the method is
   * called.
   *
   * @param stageName    stage name: e.g. ~ or table name or stage name
   * @param destPrefix   path prefix under which the data should be uploaded on the stage
   * @param inputStream  input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @param compressData compress data or not before uploading stream
   * @throws SQLException failed to compress and put data from a stream at stage
   */
  public void uploadStream(String stageName,
                           String destPrefix,
                           InputStream inputStream,
                           String destFileName,
                           boolean compressData)
  throws SQLException
  {
    uploadStreamInternal(stageName, destPrefix, inputStream,
                         destFileName, compressData);
  }

  /**
   * Method to compress data from a stream and upload it at a stage location.
   * The data will be uploaded as one file. No splitting is done in this method.
   * <p>
   * caller is responsible for releasing the inputStream after the method is
   * called.
   * <p>
   * This method is deprecated
   *
   * @param stageName    stage name: e.g. ~ or table name or stage name
   * @param destPrefix   path prefix under which the data should be uploaded on the stage
   * @param inputStream  input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @throws SQLException failed to compress and put data from a stream at stage
   */
  @Deprecated
  public void compressAndUploadStream(String stageName,
                                      String destPrefix,
                                      InputStream inputStream,
                                      String destFileName)
  throws SQLException
  {
    uploadStreamInternal(stageName, destPrefix, inputStream,
                         destFileName, true);
  }

  /**
   * Method to put data from a stream at a stage location. The data will be
   * uploaded as one file. No splitting is done in this method.
   * <p>
   * Stream size must match the total size of data in the input stream unless
   * compressData parameter is set to true.
   * <p>
   * caller is responsible for passing the correct size for the data in the
   * stream and releasing the inputStream after the method is called.
   *
   * @param stageName    stage name: e.g. ~ or table name or stage name
   * @param destPrefix   path prefix under which the data should be uploaded on the stage
   * @param inputStream  input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @param compressData whether compression is requested fore uploading data
   * @throws SQLException raises if any error occurs
   */
  private void uploadStreamInternal(String stageName,
                                    String destPrefix,
                                    InputStream inputStream,
                                    String destFileName,
                                    boolean compressData)
  throws SQLException
  {
    logger.debug("upload data from stream: stageName={}" +
                 ", destPrefix={}, destFileName={}",
                 stageName, destPrefix, destFileName);

    if (stageName == null)
    {
      throw new SnowflakeSQLException(
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "stage name is null");
    }

    if (destFileName == null)
    {
      throw new SnowflakeSQLException(
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "stage name is null");
    }

    SnowflakeStatementV1 stmt = this.createStatement().unwrap(SnowflakeStatementV1.class);

    StringBuilder putCommand = new StringBuilder();

    // use a placeholder for source file
    putCommand.append("put file:///tmp/placeholder ");

    // add stage name
    if (!stageName.startsWith("@"))
    {
      putCommand.append("@");
    }
    putCommand.append(stageName);

    // add dest prefix
    if (destPrefix != null)
    {
      if (!destPrefix.startsWith("/"))
      {
        putCommand.append("/");
      }
      putCommand.append(destPrefix);
    }

    putCommand.append(" overwrite=true");

    SnowflakeFileTransferAgent transferAgent = null;
    transferAgent = new SnowflakeFileTransferAgent(putCommand.toString(),
                                                   sfSession, stmt.getSfStatement());

    transferAgent.setSourceStream(inputStream);
    transferAgent.setDestFileNameForStreamSource(destFileName);
    transferAgent.setCompressSourceFromStream(compressData);
    transferAgent.execute();

    stmt.close();
  }

  /**
   * Download file from the given stage and return an input stream
   *
   * @param stageName      stage name
   * @param sourceFileName file path in stage
   * @param decompress     true if file compressed
   * @return an input stream
   * @throws SnowflakeSQLException if any SQL error occurs.
   */
  public InputStream downloadStream(String stageName, String sourceFileName,
                                    boolean decompress) throws SQLException

  {
    logger.debug("download data to stream: stageName={}" +
                 ", sourceFileName={}",
                 stageName, sourceFileName);

    if (Strings.isNullOrEmpty(stageName))
    {
      throw new SnowflakeSQLException(
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "stage name is null or empty");
    }

    if (Strings.isNullOrEmpty(sourceFileName))
    {
      throw new SnowflakeSQLException(
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "source file name is null or empty");
    }

    SnowflakeStatementV1 stmt = new SnowflakeStatementV1(
        this,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT);

    StringBuilder getCommand = new StringBuilder();

    getCommand.append("get ");

    if (!stageName.startsWith("@"))
    {
      getCommand.append("@");
    }

    getCommand.append(stageName);

    getCommand.append("/");

    if (sourceFileName.startsWith("/"))
    {
      sourceFileName = sourceFileName.substring(1);
    }

    getCommand.append(sourceFileName);

    //this is a fake path, used to form Get query and retrieve stage info,
    //no file will be downloaded to this location
    getCommand.append(" file:///tmp/ /*jdbc download stream*/");


    SnowflakeFileTransferAgent transferAgent =
        new SnowflakeFileTransferAgent(getCommand.toString(), sfSession,
                                       stmt.getSfStatement());

    InputStream stream = transferAgent.downloadStream(sourceFileName);

    if (decompress)
    {
      try
      {
        return new GZIPInputStream(stream);
      }
      catch (IOException ex)
      {
        throw new SnowflakeSQLException(
            SqlState.INTERNAL_ERROR,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            ex.getMessage());
      }
    }
    else
    {
      return stream;
    }
  }

  public void setInjectedDelay(int delay) throws SQLException
  {
    raiseSQLExceptionIfConnectionIsClosed();
    sfSession.setInjectedDelay(delay);
  }

  void injectedDelay() throws SQLException
  {
    raiseSQLExceptionIfConnectionIsClosed();

    int d = _injectedDelay.get();

    if (d != 0)
    {
      _injectedDelay.set(0);
      try
      {
        logger.trace("delayed for {}", d);

        Thread.sleep(d);
      }
      catch (InterruptedException ex)
      {
      }
    }
  }

  public void setInjectFileUploadFailure(String fileToFail) throws SQLException
  {
    raiseSQLExceptionIfConnectionIsClosed();
    sfSession.setInjectFileUploadFailure(fileToFail);
  }

  public SFSession getSfSession()
  {
    return sfSession;
  }

  private void appendWarning(SQLWarning w)
  {
    if (sqlWarnings == null)
    {
      sqlWarnings = w;
    }
    else
    {
      sqlWarnings.setNextWarning(w);
    }
  }

  private void appendWarnings(List<SFException> warnings)
  {
    for (SFException e : warnings)
    {
      appendWarning(new SQLWarning(e.getMessage(),
                                   e.getSqlState(),
                                   e.getVendorCode()));
    }
  }
}
