package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeSQLLoggedException.sendTelemetryData;

import java.sql.SQLFeatureNotSupportedException;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.common.core.SqlState;

public class SnowflakeLoggedFeatureNotSupportedException extends SQLFeatureNotSupportedException {

  public SnowflakeLoggedFeatureNotSupportedException(SFBaseSession session) {
    super();
    sendTelemetryData(null, SqlState.FEATURE_NOT_SUPPORTED, -1, session, this);
  }

  public SnowflakeLoggedFeatureNotSupportedException(SFBaseSession session, String message) {
    super(message);
    sendTelemetryData(null, SqlState.FEATURE_NOT_SUPPORTED, -1, session, this);
  }
}
