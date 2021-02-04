package net.snowflake.client.jdbc;

import java.sql.*;
import java.util.Properties;
import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFBaseStatement;

/**
 * Class that presents the implementation of a Snowflake Connection. This allows for alternate
 * definitions of SFSession, SFStatement, and SFResultSet, (representing the 'physical'
 * implementation layer) that can share high-level code.
 */
public interface SFConnectionHandler {

  /**
   * Whether this Connection supports asynchronous queries. If yes, createAsyncResultSet may be
   * called.
   */
  boolean supportsAsyncQuery();

  /** Initializes the SnowflakeConnection */
  void initializeConnection(String url, Properties info) throws SQLException;

  /** Gets the SFBaseSession implementation for this connection implementation */
  SFBaseSession getSFSession();

  /** Returns the SFStatementInterface implementation for this connection implementation */
  SFBaseStatement getSFStatement() throws SQLException;

  /** Creates a result set from a query id. */
  ResultSet createResultSet(String queryID, Statement statement) throws SQLException;

  /**
   * Creates a SnowflakeResultSet from a base SFBaseResultSet for this connection implementation.
   */
  SnowflakeBaseResultSet createResultSet(SFBaseResultSet resultSet, Statement statement)
      throws SQLException;

  /**
   * Creates an asynchronous result set from a base SFBaseResultSet for this connection
   * implementation.
   */
  SnowflakeBaseResultSet createAsyncResultSet(SFBaseResultSet resultSet, Statement statement)
      throws SQLException;

  /**
   * @param command The command to parse for this file transfer (e.g., PUT/GET)
   * @param statement The statement to use for this file transfer
   */
  SFBaseFileTransferAgent getFileTransferAgent(String command, SFBaseStatement statement)
      throws SQLNonTransientConnectionException, SnowflakeSQLException;
}
