package net.snowflake.client.jdbc;

import java.sql.*;
import java.util.Properties;
import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFSessionInterface;
import net.snowflake.client.core.SFStatementInterface;

/**
 * Factory class that presents the implementation of a Snowflake Connection. This allows for
 * alternate definitions of SFSession, SFStatement, and SFResultSet, (representing the 'physical'
 * implementation layer) that can share high-level code.
 */
public interface ConnectionHandler {

  /**
   * Whether this Connection supports asynchronous queries. If yes, createAsyncResultSet may be
   * called.
   */
  boolean supportsAsyncQuery();

  /** Initializes the SnowflakeConnection */
  void initializeConnection(String url, Properties info) throws SQLException;

  /** Gets the SFSessionInterface implementation for this connection implementation */
  SFSessionInterface getSFSession();

  /** Returns the SFStatementInterface implementation for this connection implementation */
  SFStatementInterface getSFStatement() throws SQLException;

  /** Creates a result set from a query id. */
  ResultSet createResultSet(String queryID, Connection connection) throws SQLException;

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
}
