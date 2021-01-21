package net.snowflake.client.jdbc;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import net.snowflake.client.core.SessionHandler;
import net.snowflake.client.core.StatementHandler;

/**
 * Factory class that presents the implementation of a Snowflake Connection. This allows for
 * alternate definitions of SFSession, SFStatement, and SFResultSet, (representing the 'physical'
 * implementation layer) that can share high-level code.
 */
public interface ConnectionHandler {
  /** Gets the SFSession implementation for this connection implementation */
  SessionHandler getSessionHandler();

  /** Returns the SFStatement implementation for this connection implementation */
  StatementHandler getStatementHandler() throws SQLException;

  /**
   * @param command The command to parse for this file transfer (e.g., PUT/GET)
   * @param statement The statement to use for this file transfer
   */
  FileTransferHandler getFileTransferHandler(
      String command, StatementHandler statement)
      throws SQLNonTransientConnectionException, SnowflakeSQLException;
}
