package net.snowflake.client.jdbc;

import java.sql.SQLNonTransientConnectionException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFSessionImpl;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.core.SFStatementImpl;

public class DefaultConnectionImpl implements ConnectionImplementationFactory {

  private SFSessionImpl sfSession;

  public DefaultConnectionImpl(SnowflakeConnectString conStr) {
    this.sfSession = new SFSessionImpl();
    sfSession.setSnowflakeConnectionString(conStr);
  }

  @Override
  public SFSession getSFSession() {
    return sfSession;
  }

  @Override
  public SFStatement createSFStatement() {
    return new SFStatementImpl(sfSession);
  }

  @Override
  public SnowflakeFileTransferAgent getFileTransferAgent(String command, SFStatement statement)
      throws SQLNonTransientConnectionException, SnowflakeSQLException {
    if (!(statement instanceof SFStatementImpl)) {
      throw new SQLNonTransientConnectionException("Internal error: Invalid SFStatement type.");
    }
    return new SnowflakeFileTransferAgentImpl(command, sfSession, (SFStatementImpl) statement);
  }
}
