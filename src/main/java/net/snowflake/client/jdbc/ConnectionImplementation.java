package net.snowflake.client.jdbc;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import net.snowflake.client.core.SFSessionInterface;
import net.snowflake.client.core.SFStatementInterface;

/**
 * Factory class that presents the implementation of a Snowflake Connection. This allows for
 * alternate definitions of SFSession, SFStatement, and SFResultSet, (representing the 'physical'
 * implementation layer) that can share high-level code.
 */
public interface ConnectionImplementation {
  /** Gets the SFSession implementation for this connection implementation */
  SFSessionInterface getSFSession();

  /** Returns the SFStatement implementation for this connection implementation */
  SFStatementInterface getSFStatement() throws SQLException;

  /**
   * @param command The command to parse for this file transfer (e.g., PUT/GET)
   * @param statement The statement to use for this file transfer
   */
  SnowflakeFileTransferAgentInterface getFileTransferAgent(
      String command, SFStatementInterface statement)
      throws SQLNonTransientConnectionException, SnowflakeSQLException;
}
