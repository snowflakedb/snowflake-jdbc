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
public interface ConnectionImplementationFactory {
  SFSessionInterface getSFSession();

  SFStatementInterface createSFStatement() throws SQLException;

  SnowflakeFileTransferAgentInterface getFileTransferAgent(String command, SFStatementInterface statement)
      throws SQLNonTransientConnectionException, SnowflakeSQLException;
}
