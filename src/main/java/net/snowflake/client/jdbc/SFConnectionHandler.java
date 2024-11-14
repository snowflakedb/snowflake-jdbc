package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
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
   * @return Whether this Connection supports asynchronous queries. If yes, createAsyncResultSet may
   *     be called.
   */
  boolean supportsAsyncQuery();

  /**
   * Initializes the SnowflakeConnection
   *
   * @param url url string
   * @param info connection parameters
   * @throws SQLException if any error is encountered
   */
  void initializeConnection(String url, Properties info) throws SQLException;

  /**
   * @return Gets the SFBaseSession implementation for this connection implementation
   */
  SFBaseSession getSFSession();

  /**
   * @return Returns the SFStatementInterface implementation for this connection implementation
   * @throws SQLException if any error occurs
   */
  SFBaseStatement getSFStatement() throws SQLException;

  /**
   * Creates a result set from a query id.
   *
   * @param queryID the query ID
   * @param statement Statement object
   * @return ResultSet
   * @throws SQLException if any error occurs
   */
  ResultSet createResultSet(String queryID, Statement statement) throws SQLException;

  /**
   * @param resultSet SFBaseResultSet
   * @param statement Statement
   * @return Creates a SnowflakeResultSet from a base SFBaseResultSet for this connection
   *     implementation.
   * @throws SQLException if an error occurs
   */
  SnowflakeBaseResultSet createResultSet(SFBaseResultSet resultSet, Statement statement)
      throws SQLException;

  /**
   * Creates an asynchronous result set from a base SFBaseResultSet for this connection
   * implementation.
   *
   * @param resultSet SFBaseResultSet
   * @param statement Statement
   * @return An asynchronous result set from SFBaseResultSet
   * @throws SQLException if an error occurs
   */
  SnowflakeBaseResultSet createAsyncResultSet(SFBaseResultSet resultSet, Statement statement)
      throws SQLException;

  /**
   * @param command The command to parse for this file transfer (e.g., PUT/GET)
   * @param statement The statement to use for this file transfer
   * @return SFBaseFileTransferAgent
   * @throws SQLNonTransientConnectionException if a connection error occurs
   * @throws SnowflakeSQLException if any other exception occurs
   */
  SFBaseFileTransferAgent getFileTransferAgent(String command, SFBaseStatement statement)
      throws SQLNonTransientConnectionException, SnowflakeSQLException;

  /**
   * Overridable method that allows for different connection implementations to use different stage
   * names for binds uploads. By default, it uses SYSTEM$BIND
   *
   * @return The name of the identifier with which a temporary stage is created in the Session for
   *     uploading array bind values.
   */
  default String getBindStageName() {
    return "SYSTEM$BIND";
  }
  ;
}
