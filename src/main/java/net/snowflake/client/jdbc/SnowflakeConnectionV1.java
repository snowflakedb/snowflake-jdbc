/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.ErrorCode.FEATURE_UNSUPPORTED;
import static net.snowflake.client.jdbc.ErrorCode.INVALID_CONNECT_STRING;

import com.google.common.base.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.log.SFLoggerUtil;
import net.snowflake.common.core.SqlState;

/** Snowflake connection implementation */
public class SnowflakeConnectionV1 implements Connection, SnowflakeConnection {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeConnectionV1.class);

  static {
    SFLoggerUtil.initializeSnowflakeLogger();
  }

  /** Refer to all created and open statements from this connection */
  private final Set<Statement> openStatements = ConcurrentHashMap.newKeySet();
  // Injected delay for the purpose of connection timeout testing
  // Any statement execution will sleep for the specified number of milliseconds
  private final AtomicInteger _injectedDelay = new AtomicInteger(0);
  private boolean isClosed;
  private SQLWarning sqlWarnings = null;
  private List<DriverPropertyInfo> missingProperties = null;
  /**
   * Amount of milliseconds a user is willing to tolerate for network related issues (e.g. HTTP
   * 503/504) or database transient issues (e.g. GS not responding)
   *
   * <p>A value of 0 means no timeout
   *
   * <p>Default: 300 seconds
   */
  private int networkTimeoutInMilli = 0; // in milliseconds
  /* this should be set to Connection.TRANSACTION_READ_COMMITTED
   * There may not be many implications here since the call to
   * setTransactionIsolation doesn't do anything.
   */
  private int transactionIsolation = Connection.TRANSACTION_NONE;
  private SFBaseSession sfSession;
  /** The SnowflakeConnectionImpl that provides the underlying physical-layer implementation */
  private SFConnectionHandler sfConnectionHandler;

  private boolean showStatementParameters;

  /**
   * Instantiates a SnowflakeConnectionV1 with the passed-in SnowflakeConnectionImpl.
   *
   * @param sfConnectionHandler The SnowflakeConnectionImpl.
   */
  public SnowflakeConnectionV1(SFConnectionHandler sfConnectionHandler) throws SQLException {
    initConnectionWithImpl(sfConnectionHandler, null, null);
  }

  /**
   * Instantiates a SnowflakeConnectionV1 with the passed-in SnowflakeConnectionImpl.
   *
   * @param sfConnectionHandler The SnowflakeConnectionImpl.
   */
  public SnowflakeConnectionV1(SFConnectionHandler sfConnectionHandler, String url, Properties info)
      throws SQLException {
    initConnectionWithImpl(sfConnectionHandler, url, info);
  }

  /**
   * A connection will establish a session token from snowflake
   *
   * @param url server url used to create snowflake connection
   * @param info properties about the snowflake connection
   * @throws SQLException if failed to create a snowflake connection i.e. username or password not
   *     specified
   */
  public SnowflakeConnectionV1(String url, Properties info) throws SQLException {
    SnowflakeConnectString conStr = SnowflakeConnectString.parse(url, info);
    if (!conStr.isValid()) {
      throw new SnowflakeSQLException(INVALID_CONNECT_STRING, url);
    }

    initConnectionWithImpl(new DefaultSFConnectionHandler(conStr), url, info);
    appendWarnings(sfSession.getSqlWarnings());
    isClosed = false;
  }

  public SnowflakeConnectionV1(String url, Properties info, boolean fakeConnection)
      throws SQLException {
    SnowflakeConnectString conStr = SnowflakeConnectString.parse(url, info);
    if (!conStr.isValid()) {
      throw new SnowflakeSQLException(
          SqlState.CONNECTION_EXCEPTION, INVALID_CONNECT_STRING.getMessageCode(), url);
    }

    initConnectionWithImpl(new DefaultSFConnectionHandler(conStr, true), url, info);
    isClosed = false;
  }

  private void initConnectionWithImpl(
      SFConnectionHandler sfConnectionHandler, String url, Properties info) throws SQLException {
    this.sfConnectionHandler = sfConnectionHandler;
    sfConnectionHandler.initializeConnection(url, info);
    this.sfSession = sfConnectionHandler.getSFSession();
    missingProperties = sfSession.checkProperties();
    this.showStatementParameters = sfSession.getPreparedStatementLogging();
  }

  public List<DriverPropertyInfo> returnMissingProperties() {
    return missingProperties;
  }

  private void raiseSQLExceptionIfConnectionIsClosed() throws SQLException {
    if (isClosed) {
      throw new SnowflakeSQLException(ErrorCode.CONNECTION_CLOSED);
    }
  }

  /**
   * Execute a statement where the result isn't needed, and the statement is closed before this
   * method returns
   *
   * @param stmtText text of the statement
   * @throws SQLException exception thrown it the statement fails to execute
   */
  private void executeImmediate(String stmtText) throws SQLException {
    // execute the statement and auto-close it as well
    try (final Statement statement = this.createStatement()) {
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
  public Statement createStatement() throws SQLException {
    raiseSQLExceptionIfConnectionIsClosed();
    Statement stmt = createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    openStatements.add(stmt);
    return stmt;
  }

  /**
   * Get an instance of a ResultSet object
   *
   * @param queryID
   * @return
   * @throws SQLException
   */
  public ResultSet createResultSet(String queryID) throws SQLException {
    raiseSQLExceptionIfConnectionIsClosed();
    ResultSet rs = sfConnectionHandler.createResultSet(queryID, createStatement());
    return rs;
  }

  /**
   * Close the connection
   *
   * @throws SQLException failed to close the connection
   */
  @Override
  public void close() throws SQLException {
    logger.debug(" public void close()");

    if (isClosed) {
      // No exception is raised even if the connection is closed.
      return;
    }

    isClosed = true;
    try {
      if (sfSession != null && sfSession.isSafeToClose()) {
        sfSession.close();
        sfSession = null;
      }
      // make sure to close all created statements
      for (Statement stmt : openStatements) {
        if (stmt != null && !stmt.isClosed()) {
          if (stmt.isWrapperFor(SnowflakeStatementV1.class)) {
            stmt.unwrap(SnowflakeStatementV1.class).close(false);
          } else {
            stmt.close();
          }
        }
      }
      openStatements.clear();

    } catch (SFException ex) {
      throw new SnowflakeSQLLoggedException(
          sfSession, ex.getSqlState(), ex.getVendorCode(), ex.getCause(), ex.getParams());
    }
  }

  public String getSessionID() throws SQLException {
    if (isClosed) {
      raiseSQLExceptionIfConnectionIsClosed();
    }
    return sfSession.getSessionId();
  }

  @Override
  public boolean isClosed() throws SQLException {
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
  public DatabaseMetaData getMetaData() throws SQLException {
    logger.debug(" public DatabaseMetaData getMetaData()");
    raiseSQLExceptionIfConnectionIsClosed();
    return new SnowflakeDatabaseMetaData(this);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    logger.debug(" public CallableStatement prepareCall(String sql)");
    raiseSQLExceptionIfConnectionIsClosed();
    CallableStatement stmt = prepareCall(sql, false);
    openStatements.add(stmt);
    return stmt;
  }

  public CallableStatement prepareCall(String sql, boolean skipParsing) throws SQLException {
    logger.debug(" public CallableStatement prepareCall(String sql, boolean skipParsing)");
    raiseSQLExceptionIfConnectionIsClosed();
    CallableStatement stmt =
        new SnowflakeCallableStatementV1(
            this,
            sql,
            skipParsing,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT);
    openStatements.add(stmt);
    return stmt;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    logger.debug(
        " public CallableStatement prepareCall(String sql,"
            + " int resultSetType,int resultSetConcurrency");
    CallableStatement stmt =
        prepareCall(sql, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    openStatements.add(stmt);
    return stmt;
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    logger.debug(" public CallableStatement prepareCall(String sql, int " + "resultSetType,");
    CallableStatement stmt =
        new SnowflakeCallableStatementV1(
            this, sql, false, resultSetType, resultSetConcurrency, resultSetHoldability);
    openStatements.add(stmt);
    return stmt;
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    logger.debug("public String nativeSQL(String sql)");
    raiseSQLExceptionIfConnectionIsClosed();
    return sql;
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    logger.debug("boolean getAutoCommit()");
    raiseSQLExceptionIfConnectionIsClosed();
    return sfSession.getAutoCommit();
  }

  @Override
  public void setAutoCommit(boolean isAutoCommit) throws SQLException {
    logger.debug("void setAutoCommit(boolean isAutoCommit)");
    boolean currentAutoCommit = this.getAutoCommit();
    if (isAutoCommit != currentAutoCommit) {
      sfSession.setAutoCommit(isAutoCommit);
      this.executeImmediate(
          "alter session /* JDBC:SnowflakeConnectionV1.setAutoCommit*/ set autocommit="
              + isAutoCommit);
    }
  }

  @Override
  public void commit() throws SQLException {
    logger.debug("void commit()");
    this.executeImmediate("commit");
  }

  @Override
  public void rollback() throws SQLException {
    logger.debug("void rollback()");
    this.executeImmediate("rollback");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    logger.debug("void rollback(Savepoint savepoint)");

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    logger.debug("boolean isReadOnly()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    logger.debug("void setReadOnly(boolean readOnly)");
    raiseSQLExceptionIfConnectionIsClosed();
    if (readOnly) {

      throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
    }
  }

  @Override
  public String getCatalog() throws SQLException {
    raiseSQLExceptionIfConnectionIsClosed();
    return sfSession.getDatabase();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    logger.debug("void setCatalog(String catalog)");

    // switch db by running "use db"
    this.executeImmediate("use database \"" + catalog + "\"");
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    logger.debug("int getTransactionIsolation()");
    raiseSQLExceptionIfConnectionIsClosed();
    return this.transactionIsolation;
  }

  /**
   * Sets the transaction isolation level.
   *
   * @param level transaction level: TRANSACTION_NONE or TRANSACTION_READ_COMMITTED
   * @throws SQLException if any SQL error occurs
   */
  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    logger.debug("void setTransactionIsolation(int level), level = {}", level);
    raiseSQLExceptionIfConnectionIsClosed();
    if (level == Connection.TRANSACTION_NONE || level == Connection.TRANSACTION_READ_COMMITTED) {
      this.transactionIsolation = level;
    } else {
      throw new SQLFeatureNotSupportedException(
          "Transaction Isolation " + level + " not supported.",
          FEATURE_UNSUPPORTED.getSqlState(),
          FEATURE_UNSUPPORTED.getMessageCode());
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    logger.debug("SQLWarning getWarnings()");
    raiseSQLExceptionIfConnectionIsClosed();
    return sqlWarnings;
  }

  @Override
  public void clearWarnings() throws SQLException {
    logger.debug("void clearWarnings()");
    raiseSQLExceptionIfConnectionIsClosed();
    sfSession.clearSqlWarnings();
    sqlWarnings = null;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    logger.debug("Statement createStatement(int resultSetType, " + "int resultSetConcurrency)");

    Statement stmt =
        createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    openStatements.add(stmt);
    return stmt;
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    logger.debug(
        "Statement createStatement(int resultSetType, "
            + "int resultSetConcurrency, int resultSetHoldability");

    Statement stmt =
        new SnowflakeStatementV1(this, resultSetType, resultSetConcurrency, resultSetHoldability);
    openStatements.add(stmt);
    return stmt;
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    logger.debug("PreparedStatement prepareStatement(String sql)");
    raiseSQLExceptionIfConnectionIsClosed();
    PreparedStatement stmt = prepareStatement(sql, false);
    openStatements.add(stmt);
    return stmt;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    logger.debug("PreparedStatement prepareStatement(String sql, " + "int autoGeneratedKeys)");

    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return prepareStatement(sql);
    }

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    logger.debug("PreparedStatement prepareStatement(String sql, " + "int[] columnIndexes)");

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    logger.debug("PreparedStatement prepareStatement(String sql, " + "String[] columnNames)");

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    logger.debug("PreparedStatement prepareStatement(String sql, " + "int resultSetType,");

    PreparedStatement stmt =
        prepareStatement(
            sql, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    openStatements.add(stmt);
    return stmt;
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    logger.debug("PreparedStatement prepareStatement(String sql, " + "int resultSetType,");

    PreparedStatement stmt =
        new SnowflakePreparedStatementV1(
            this, sql, false, resultSetType, resultSetConcurrency, resultSetHoldability);
    openStatements.add(stmt);
    return stmt;
  }

  public PreparedStatement prepareStatement(String sql, boolean skipParsing) throws SQLException {
    logger.debug("PreparedStatement prepareStatement(String sql, boolean skipParsing)");
    raiseSQLExceptionIfConnectionIsClosed();
    PreparedStatement stmt =
        new SnowflakePreparedStatementV1(
            this,
            sql,
            skipParsing,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT);
    openStatements.add(stmt);
    return stmt;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    raiseSQLExceptionIfConnectionIsClosed();
    return Collections.emptyMap(); // nop
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public int getHoldability() throws SQLException {
    raiseSQLExceptionIfConnectionIsClosed();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT; // nop
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public Blob createBlob() throws SQLException {

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public Clob createClob() throws SQLException {
    raiseSQLExceptionIfConnectionIsClosed();
    return new SnowflakeClob();
  }

  @Override
  public NClob createNClob() throws SQLException {

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    // TODO: run query here or ping
    if (timeout < 0) {
      throw new SQLException("timeout is less than 0");
    }
    return !isClosed; // no exception is raised
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    Map<String, ClientInfoStatus> failedProps = new HashMap<>();
    failedProps.put(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
    raiseSetClientInfoException(failedProps);
  }

  private void raiseSetClientInfoException(Map<String, ClientInfoStatus> failedProps)
      throws SQLClientInfoException {
    if (isClosed) {
      throw new SQLClientInfoException(
          "The connection is not opened.",
          ErrorCode.CONNECTION_CLOSED.getSqlState(),
          ErrorCode.CONNECTION_CLOSED.getMessageCode(),
          failedProps);
    }

    throw new SQLClientInfoException(
        "The client property cannot be set by setClientInfo.",
        ErrorCode.INVALID_PARAMETER_VALUE.getSqlState(),
        ErrorCode.INVALID_PARAMETER_VALUE.getMessageCode(),
        failedProps);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    logger.debug("Properties getClientInfo()");
    raiseSQLExceptionIfConnectionIsClosed();
    // sfSession must not be null if the connection is not closed.
    return sfSession.getClientInfo();
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    Map<String, ClientInfoStatus> failedProps = new HashMap<>();
    Enumeration<?> propList = properties.propertyNames();
    while (propList.hasMoreElements()) {
      String name = (String) propList.nextElement();
      failedProps.put(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
    }
    raiseSetClientInfoException(failedProps);
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    logger.debug("String getClientInfo(String name)");

    raiseSQLExceptionIfConnectionIsClosed();
    // sfSession must not be null if the connection is not closed.
    return sfSession.getClientInfo(name);
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    logger.debug("Array createArrayOf(String typeName, Object[] " + "elements)");

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    logger.debug("Struct createStruct(String typeName, Object[] " + "attributes)");

    throw new SnowflakeLoggedFeatureNotSupportedException(sfSession);
  }

  @Override
  public String getSchema() throws SQLException {
    raiseSQLExceptionIfConnectionIsClosed();
    return sfSession.getSchema();
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    logger.debug("void setSchema(String schema)");

    String databaseName = getCatalog();

    // switch schema by running "use db.schema"
    if (databaseName == null) {
      this.executeImmediate("use schema \"" + schema + "\"");
    } else {
      this.executeImmediate("use schema \"" + databaseName + "\".\"" + schema + "\"");
    }
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    logger.debug("void abort(Executor executor)");

    close();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    logger.debug("void setNetworkTimeout(Executor executor, int " + "milliseconds)");
    raiseSQLExceptionIfConnectionIsClosed();

    networkTimeoutInMilli = milliseconds;
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    logger.debug("int getNetworkTimeout()");
    raiseSQLExceptionIfConnectionIsClosed();
    return networkTimeoutInMilli;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    logger.debug("boolean isWrapperFor(Class<?> iface)");

    return iface.isInstance(this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    logger.debug("<T> T unwrap(Class<T> iface)");

    if (!iface.isInstance(this)) {
      throw new SQLException(
          this.getClass().getName() + " not unwrappable from " + iface.getName());
    }
    return (T) this;
  }

  int getDatabaseMajorVersion() {
    return sfSession.getDatabaseMajorVersion();
  }

  int getDatabaseMinorVersion() {
    return sfSession.getDatabaseMinorVersion();
  }

  String getDatabaseVersion() {
    return sfSession.getDatabaseVersion();
  }

  @Override
  public SFConnectionHandler getHandler() {
    return sfConnectionHandler;
  }

  /**
   * Method to put data from a stream at a stage location. The data will be uploaded as one file. No
   * splitting is done in this method.
   *
   * <p>Stream size must match the total size of data in the input stream unless compressData
   * parameter is set to true.
   *
   * <p>caller is responsible for passing the correct size for the data in the stream and releasing
   * the inputStream after the method is called.
   *
   * <p>Note this method is deprecated since streamSize is not required now. Keep the function
   * signature for backward compatibility
   *
   * @param stageName stage name: e.g. ~ or table name or stage name
   * @param destPrefix path prefix under which the data should be uploaded on the stage
   * @param inputStream input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @param streamSize data size in the stream
   * @throws SQLException failed to put data from a stream at stage
   */
  @Deprecated
  public void uploadStream(
      String stageName,
      String destPrefix,
      InputStream inputStream,
      String destFileName,
      long streamSize)
      throws SQLException {
    uploadStreamInternal(stageName, destPrefix, inputStream, destFileName, false);
  }

  /**
   * Method to compress data from a stream and upload it at a stage location. The data will be
   * uploaded as one file. No splitting is done in this method.
   *
   * <p>caller is responsible for releasing the inputStream after the method is called.
   *
   * @param stageName stage name: e.g. ~ or table name or stage name
   * @param destPrefix path prefix under which the data should be uploaded on the stage
   * @param inputStream input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @param compressData compress data or not before uploading stream
   * @throws SQLException failed to compress and put data from a stream at stage
   */
  public void uploadStream(
      String stageName,
      String destPrefix,
      InputStream inputStream,
      String destFileName,
      boolean compressData)
      throws SQLException {
    uploadStreamInternal(stageName, destPrefix, inputStream, destFileName, compressData);
  }

  /**
   * Method to compress data from a stream and upload it at a stage location. The data will be
   * uploaded as one file. No splitting is done in this method.
   *
   * <p>caller is responsible for releasing the inputStream after the method is called.
   *
   * <p>This method is deprecated
   *
   * @param stageName stage name: e.g. ~ or table name or stage name
   * @param destPrefix path prefix under which the data should be uploaded on the stage
   * @param inputStream input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @throws SQLException failed to compress and put data from a stream at stage
   */
  @Deprecated
  public void compressAndUploadStream(
      String stageName, String destPrefix, InputStream inputStream, String destFileName)
      throws SQLException {
    uploadStreamInternal(stageName, destPrefix, inputStream, destFileName, true);
  }

  /**
   * Method to put data from a stream at a stage location. The data will be uploaded as one file. No
   * splitting is done in this method.
   *
   * <p>Stream size must match the total size of data in the input stream unless compressData
   * parameter is set to true.
   *
   * <p>caller is responsible for passing the correct size for the data in the stream and releasing
   * the inputStream after the method is called.
   *
   * @param stageName stage name: e.g. ~ or table name or stage name
   * @param destPrefix path prefix under which the data should be uploaded on the stage
   * @param inputStream input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @param compressData whether compression is requested fore uploading data
   * @throws SQLException raises if any error occurs
   */
  private void uploadStreamInternal(
      String stageName,
      String destPrefix,
      InputStream inputStream,
      String destFileName,
      boolean compressData)
      throws SQLException {
    logger.debug(
        "upload data from stream: stageName={}" + ", destPrefix={}, destFileName={}",
        stageName,
        destPrefix,
        destFileName);

    if (stageName == null) {
      throw new SnowflakeSQLLoggedException(
          sfSession,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "stage name is null");
    }

    if (destFileName == null) {
      throw new SnowflakeSQLLoggedException(
          sfSession,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "stage name is null");
    }

    SnowflakeStatementV1 stmt = this.createStatement().unwrap(SnowflakeStatementV1.class);

    StringBuilder putCommand = new StringBuilder();

    // use a placeholder for source file
    putCommand.append("put file:///tmp/placeholder ");

    // add stage name
    if (!(stageName.startsWith("@") || stageName.startsWith("'@") || stageName.startsWith("$$@"))) {
      putCommand.append("@");
    }
    putCommand.append(stageName);

    // add dest prefix
    if (destPrefix != null) {
      if (!destPrefix.startsWith("/")) {
        putCommand.append("/");
      }
      putCommand.append(destPrefix);
    }

    putCommand.append(" overwrite=true");

    SFBaseFileTransferAgent transferAgent =
        sfConnectionHandler.getFileTransferAgent(putCommand.toString(), stmt.getSFBaseStatement());
    transferAgent.setSourceStream(inputStream);
    transferAgent.setDestFileNameForStreamSource(destFileName);
    transferAgent.setCompressSourceFromStream(compressData);
    transferAgent.execute();

    stmt.close();
  }

  /**
   * Download file from the given stage and return an input stream
   *
   * @param stageName stage name
   * @param sourceFileName file path in stage
   * @param decompress true if file compressed
   * @return an input stream
   * @throws SnowflakeSQLException if any SQL error occurs.
   */
  public InputStream downloadStream(String stageName, String sourceFileName, boolean decompress)
      throws SQLException {

    logger.debug(
        "download data to stream: stageName={}" + ", sourceFileName={}", stageName, sourceFileName);

    if (Strings.isNullOrEmpty(stageName)) {
      throw new SnowflakeSQLLoggedException(
          sfSession,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "stage name is null or empty");
    }

    if (Strings.isNullOrEmpty(sourceFileName)) {
      throw new SnowflakeSQLLoggedException(
          sfSession,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "source file name is null or empty");
    }

    SnowflakeStatementV1 stmt =
        new SnowflakeStatementV1(
            this,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT);

    StringBuilder getCommand = new StringBuilder();

    getCommand.append("get ");

    if (!stageName.startsWith("@")) {
      getCommand.append("@");
    }

    getCommand.append(stageName);

    getCommand.append("/");

    if (sourceFileName.startsWith("/")) {
      sourceFileName = sourceFileName.substring(1);
    }

    getCommand.append(sourceFileName);

    // this is a fake path, used to form Get query and retrieve stage info,
    // no file will be downloaded to this location
    getCommand.append(" file:///tmp/ /*jdbc download stream*/");

    SFBaseFileTransferAgent transferAgent =
        sfConnectionHandler.getFileTransferAgent(getCommand.toString(), stmt.getSFBaseStatement());

    InputStream stream = transferAgent.downloadStream(sourceFileName);

    if (decompress) {
      try {
        return new GZIPInputStream(stream);
      } catch (IOException ex) {
        throw new SnowflakeSQLLoggedException(
            sfSession,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            SqlState.INTERNAL_ERROR,
            ex.getMessage());
      }
    } else {
      return stream;
    }
  }

  public void setInjectedDelay(int delay) throws SQLException {
    raiseSQLExceptionIfConnectionIsClosed();
    sfSession.setInjectedDelay(delay);
  }

  void injectedDelay() throws SQLException {
    raiseSQLExceptionIfConnectionIsClosed();

    int d = _injectedDelay.get();

    if (d != 0) {
      _injectedDelay.set(0);
      try {
        logger.trace("delayed for {}", d);

        Thread.sleep(d);
      } catch (InterruptedException ex) {
      }
    }
  }

  public void setInjectFileUploadFailure(String fileToFail) throws SQLException {
    raiseSQLExceptionIfConnectionIsClosed();
    sfSession.setInjectFileUploadFailure(fileToFail);
  }

  public SFBaseSession getSFBaseSession() {
    return sfSession;
  }

  // Convenience method to return an SFSession-typed SFBaseSession object, but
  // performs the type-checking as necessary.
  public SFSession getSfSession() throws SnowflakeSQLException {
    if (sfSession instanceof SFSession) {
      return (SFSession) sfSession;
    }

    throw new SnowflakeSQLException("getSFSession() called with a different SFBaseSession type.");
  }

  private void appendWarning(SQLWarning w) {
    if (sqlWarnings == null) {
      sqlWarnings = w;
    } else {
      sqlWarnings.setNextWarning(w);
    }
  }

  private void appendWarnings(List<SFException> warnings) {
    for (SFException e : warnings) {
      appendWarning(new SQLWarning(e.getMessage(), e.getSqlState(), e.getVendorCode()));
    }
  }

  public boolean getShowStatementParameters() {
    return showStatementParameters;
  }

  void removeClosedStatement(Statement stmt) {
    openStatements.remove(stmt);
  }
}
