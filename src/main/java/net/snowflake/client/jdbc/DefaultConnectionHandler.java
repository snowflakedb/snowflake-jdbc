package net.snowflake.client.jdbc;

import java.sql.SQLNonTransientConnectionException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SessionHandler;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.core.StatementHandler;

/**
 * The default ConnectionHandler used by SnowflakeConnectionV(x).
 * Unless a separate implementation is provided, a DefaultConnectionHandler will
 * be constructed automatically by the Connection class.
 */
public class DefaultConnectionHandler implements ConnectionHandler {

  private final SFSession sfSession;

  /**
   * Constructs a DefaultConnectionHandler using a SnowflakeConnectString.
   * This can be done by using SnowflakeConnectString.parse(url, info), where
   * url is a connection url and info is a java.util.Properties
   *
   * @param conStr A SnowflakeConnectString object
   */
  public DefaultConnectionHandler(SnowflakeConnectString conStr) {
    this.sfSession = new SFSession();
    sfSession.setSnowflakeConnectionString(conStr);
  }

  /**
   * Returns the default SFSession client implementation.
   */
  @Override
  public SessionHandler getSessionHandler() {
    return sfSession;
  }

  /**
   * Returns the default SFStatement client implementation.
   */
  @Override
  public StatementHandler getStatementHandler() {
    return new SFStatement(sfSession);
  }

  /**
   * Returns the default SnowflakeFileTransferAgent implementation for the client.
   *
   * @param command The command to parse for this file transfer (e.g., PUT/GET)
   * @param statement The statement to use for this file transfer
   */
  @Override
  public FileTransferHandler getFileTransferHandler(
      String command, StatementHandler statement)
      throws SQLNonTransientConnectionException, SnowflakeSQLException {
    if (!(statement instanceof SFStatement)) {
      throw new SQLNonTransientConnectionException("Internal error: Invalid SFStatement type.");
    }
    return new SnowflakeFileTransferAgent(command, sfSession, (SFStatement) statement);
  }
}
