package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFSessionInterface;
import net.snowflake.client.core.SFStatementInterface;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Properties;

/**
 * Factory class that presents the implementation of a Snowflake Connection. This allows for
 * alternate definitions of SFSession, SFStatement, and SFResultSet, (representing the 'physical'
 * implementation layer) that can share high-level code.
 */
public interface ConnectionHandler {

  /** Initializes the SnowflakeConnection */
  void initializeConnection(String url, Properties info) throws SQLException;

  /** Gets the SFSession implementation for this connection implementation */
  SFSessionInterface getSessionHandler();

  /** Returns the SFStatement implementation for this connection implementation */
  SFStatementInterface getStatementHandler() throws SQLException;

  ResultSet createResultSet(String queryID, Connection connection) throws SQLException;

  /**
   * @param command The command to parse for this file transfer (e.g., PUT/GET)
   * @param statement The statement to use for this file transfer
   */
  FileTransferHandler getFileTransferHandler(String command, SFStatementInterface statement)
      throws SQLNonTransientConnectionException, SnowflakeSQLException;
}
