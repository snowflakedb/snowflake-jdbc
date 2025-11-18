package net.snowflake.client.internal.jdbc;

import java.sql.SQLFeatureNotSupportedException;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.jdbc.telemetry.SqlExceptionTelemetryHandler;
import net.snowflake.common.core.SqlState;

public class SnowflakeLoggedFeatureNotSupportedException extends SQLFeatureNotSupportedException {

  public SnowflakeLoggedFeatureNotSupportedException(SFBaseSession session) {
    super();
    SqlExceptionTelemetryHandler.sendTelemetry(
        null, SqlState.FEATURE_NOT_SUPPORTED, -1, session, this);
  }

  public SnowflakeLoggedFeatureNotSupportedException(SFBaseSession session, String message) {
    super(message);
    SqlExceptionTelemetryHandler.sendTelemetry(
        null, SqlState.FEATURE_NOT_SUPPORTED, -1, session, this);
  }
}
