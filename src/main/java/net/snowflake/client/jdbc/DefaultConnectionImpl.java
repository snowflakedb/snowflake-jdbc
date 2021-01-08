package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFSessionImpl;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.core.SFStatementImpl;

public class DefaultConnectionImpl implements SnowflakeConnectionImpl {

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

}
