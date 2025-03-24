package net.snowflake.client.pooling;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Properties;
import java.util.concurrent.Executor;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Logical connection is wrapper class on top of SnowflakeConnectionV1 Every method call will be
 * delegated to SnowflakeConnectionV1 except for close method
 */
class LogicalConnection implements Connection {
  private static final SFLogger logger = SFLoggerFactory.getLogger(LogicalConnection.class);

  /** physical connection to snowflake, instance SnowflakeConnectionV1 */
  private final Connection physicalConnection;

  /** Pooled connection object that create this logical connection */
  private final SnowflakePooledConnection pooledConnection;

  /**
   * flags indicating whether this logical connection is closed or not Note: This is different from
   * physical connection's state of whether closed or not
   */
  private boolean isClosed;

  LogicalConnection(SnowflakePooledConnection pooledConnection) throws SQLException {
    this.physicalConnection = pooledConnection.getPhysicalConnection();
    this.pooledConnection = pooledConnection;
    this.isClosed = physicalConnection.isClosed();
  }

  @Override
  public Statement createStatement() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.createStatement();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.prepareStatement(sql);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.prepareCall(sql);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.nativeSQL(sql);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.setAutoCommit(autoCommit);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.getAutoCommit();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void commit() throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.commit();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void rollback() throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.rollback();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  /** Logical connection will not close physical connection, but fire events */
  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }
    SnowflakeConnectionV1 sfConnection = physicalConnection.unwrap(SnowflakeConnectionV1.class);
    logger.debug("Closing logical connection with session id: {}", sfConnection.getSessionID());
    pooledConnection.fireConnectionCloseEvent();
    isClosed = true;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.getMetaData();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.setReadOnly(readOnly);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.isReadOnly();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.setCatalog(catalog);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public String getCatalog() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.getCatalog();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.setTransactionIsolation(level);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.getTransactionIsolation();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.getWarnings();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void clearWarnings() throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.clearWarnings();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.createStatement(resultSetType, resultSetConcurrency);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.getTypeMap();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.setTypeMap(map);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.setHoldability(holdability);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.getHoldability();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.setSavepoint();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.setSavepoint(name);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.rollback(savepoint);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.releaseSavepoint(savepoint);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.createStatement(
          resultSetType, resultSetConcurrency, resultSetHoldability);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.prepareStatement(
          sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.prepareCall(
          sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.prepareStatement(sql, autoGeneratedKeys);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int columnIndexes[]) throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.prepareStatement(sql, columnIndexes);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String columnNames[]) throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.prepareStatement(sql, columnNames);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public Clob createClob() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.createClob();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public Blob createBlob() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.createBlob();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public NClob createNClob() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.createNClob();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.createSQLXML();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    try {
      return !isClosed && physicalConnection.isValid(timeout);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    try {
      physicalConnection.setClientInfo(name, value);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    try {
      physicalConnection.setClientInfo(properties);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    try {
      return physicalConnection.getClientInfo(name);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.getClientInfo();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.createArrayOf(typeName, elements);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.createStruct(typeName, attributes);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.setSchema(schema);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public String getSchema() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.getSchema();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    try {
      physicalConnection.abort(executor);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throwExceptionIfClosed();

    try {
      physicalConnection.setNetworkTimeout(executor, milliseconds);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    throwExceptionIfClosed();

    try {
      return physicalConnection.getNetworkTimeout();
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    try {
      return physicalConnection.isWrapperFor(iface);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      return physicalConnection.unwrap(iface);
    } catch (SQLException e) {
      pooledConnection.fireConnectionErrorEvent(e);
      throw e;
    }
  }

  private void throwExceptionIfClosed() throws SQLException {
    if (isClosed) {
      throw new SnowflakeSQLException(ErrorCode.CONNECTION_CLOSED);
    }
  }
}
