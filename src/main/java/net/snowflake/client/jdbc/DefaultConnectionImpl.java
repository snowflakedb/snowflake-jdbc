package net.snowflake.client.jdbc;

import java.sql.SQLNonTransientConnectionException;
import net.snowflake.client.core.SFSessionInterface;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatementInterface;
import net.snowflake.client.core.SFStatement;

public class DefaultConnectionImpl implements ConnectionImplementationFactory {

  private SFSession sfSession;

  public DefaultConnectionImpl(SnowflakeConnectString conStr) {
    this.sfSession = new SFSession();
    sfSession.setSnowflakeConnectionString(conStr);
  }

  @Override
  public SFSessionInterface getSFSession() {
    return sfSession;
  }

  @Override
  public SFStatementInterface createSFStatement() {
    return new SFStatement(sfSession);
  }

  @Override
  public SnowflakeFileTransferAgentInterface getFileTransferAgent(String command, SFStatementInterface statement)
      throws SQLNonTransientConnectionException, SnowflakeSQLException {
    if (!(statement instanceof SFStatement)) {
      throw new SQLNonTransientConnectionException("Internal error: Invalid SFStatement type.");
    }
    return new SnowflakeFileTransferAgent(command, sfSession, (SFStatement) statement);
  }
}
