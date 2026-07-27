package net.snowflake.client.internal.exception;

import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.jdbc.telemetry.SqlExceptionTelemetryHandler;
import net.snowflake.client.internal.jdbc.telemetry.TelemetryUtil;

/**
 * This SnowflakeSQLLoggedException class extends the SnowflakeSQLException class to add OOB
 * telemetry data for sql exceptions. Not all sql exceptions require OOB telemetry logging so the
 * exceptions in this class should only be thrown if there is a need for logging the exception with
 * OOB telemetry.
 */
public class SnowflakeSQLLoggedException extends SnowflakeSQLException {

  public SnowflakeSQLLoggedException(
      String queryID, SFSession session, String sqlState, String message, Exception cause) {
    super(queryID, cause, sqlState, TelemetryUtil.NO_VENDOR_CODE, message);
    SqlExceptionTelemetryHandler.sendTelemetry(
        queryID, sqlState, TelemetryUtil.NO_VENDOR_CODE, session, this);
  }

  /**
   * @param session SFBaseSession
   * @param reason exception reason
   * @param SQLState the SQL state
   * @param vendorCode the vendor code
   * @param queryId the query ID
   */
  public SnowflakeSQLLoggedException(
      SFBaseSession session, String reason, String SQLState, int vendorCode, String queryId) {
    super(queryId, reason, SQLState, vendorCode);
    SqlExceptionTelemetryHandler.sendTelemetry(queryId, SQLState, vendorCode, session, this);
  }

  /**
   * @param session SFBaseSession
   * @param vendorCode the vendor code
   * @param SQLState the SQL state
   */
  public SnowflakeSQLLoggedException(SFBaseSession session, int vendorCode, String SQLState) {
    super((String) null, SQLState, vendorCode);
    SqlExceptionTelemetryHandler.sendTelemetry(null, SQLState, vendorCode, session, this);
  }

  /**
   * @param queryId the query ID
   * @param session SFBaseSession
   * @param vendorCode the vendor code
   * @param SQLState the SQL state
   */
  public SnowflakeSQLLoggedException(
      String queryId, SFBaseSession session, int vendorCode, String SQLState) {
    super(queryId, SQLState, vendorCode);
    SqlExceptionTelemetryHandler.sendTelemetry(queryId, SQLState, vendorCode, session, this);
  }

  /**
   * @param session SFBaseSession
   * @param SQLState the SQL state
   * @param reason the exception reason
   */
  public SnowflakeSQLLoggedException(SFBaseSession session, String SQLState, String reason) {
    super(null, reason, SQLState);
    SqlExceptionTelemetryHandler.sendTelemetry(
        null, SQLState, TelemetryUtil.NO_VENDOR_CODE, session, this);
  }

  /**
   * @param queryId the query ID
   * @param session SFBaseSession
   * @param SQLState the SQL state
   * @param reason the exception reason
   */
  public SnowflakeSQLLoggedException(
      String queryId, SFBaseSession session, String SQLState, String reason) {
    super(null, reason, SQLState);
    SqlExceptionTelemetryHandler.sendTelemetry(
        queryId, SQLState, TelemetryUtil.NO_VENDOR_CODE, session, this);
  }

  /**
   * @param session SFBaseSession
   * @param vendorCode the vendor code
   * @param SQLState the SQL state
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(
      SFBaseSession session, int vendorCode, String SQLState, Object... params) {
    this(null, session, vendorCode, SQLState, params);
  }

  /**
   * @param queryId the query ID
   * @param session SFBaseSession
   * @param vendorCode the vendor code
   * @param SQLState the SQL state
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(
      String queryId, SFBaseSession session, int vendorCode, String SQLState, Object... params) {
    super(queryId, SQLState, vendorCode, params);
    SqlExceptionTelemetryHandler.sendTelemetry(queryId, SQLState, vendorCode, session, this);
  }

  /**
   * @param session SFBaseSession
   * @param errorCode the error code
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(SFBaseSession session, ErrorCode errorCode, Object... params) {
    super(errorCode, params);
    SqlExceptionTelemetryHandler.sendTelemetry(
        null, errorCode.getSqlState(), errorCode.getMessageCode(), session, this);
  }

  /**
   * @param session SFBaseSession
   * @param errorCode the error code
   * @param ex Throwable exception
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(
      SFBaseSession session, ErrorCode errorCode, Throwable ex, Object... params) {
    super(ex, errorCode, params);
    SqlExceptionTelemetryHandler.sendTelemetry(
        null, errorCode.getSqlState(), errorCode.getMessageCode(), session, this);
  }

  /**
   * @param session SFBaseSession
   * @param SQLState the SQL state
   * @param vendorCode the vendor code
   * @param ex Throwable exception
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(
      SFBaseSession session, String SQLState, int vendorCode, Throwable ex, Object... params) {
    super(ex, SQLState, vendorCode, params);
    SqlExceptionTelemetryHandler.sendTelemetry(null, SQLState, vendorCode, session, this);
  }

  /**
   * @param queryId the query ID
   * @param session SFBaseSession
   * @param SQLState the SQL state
   * @param vendorCode the vendor code
   * @param ex Throwable exception
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(
      String queryId,
      SFBaseSession session,
      String SQLState,
      int vendorCode,
      Throwable ex,
      Object... params) {
    super(queryId, ex, SQLState, vendorCode, params);
    SqlExceptionTelemetryHandler.sendTelemetry(queryId, SQLState, vendorCode, session, this);
  }

  /**
   * @param queryId the query ID
   * @param session SFBaseSession
   * @param errorCode the error code
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(
      String queryId, SFBaseSession session, ErrorCode errorCode, Object... params) {
    super(queryId, errorCode, params);
    SqlExceptionTelemetryHandler.sendTelemetry(
        queryId, null, TelemetryUtil.NO_VENDOR_CODE, session, this);
  }

  /**
   * @param session SFBaseSession
   * @param e throwable exception
   */
  public SnowflakeSQLLoggedException(SFBaseSession session, SFException e) {
    super(e);
    SqlExceptionTelemetryHandler.sendTelemetry(
        null, null, TelemetryUtil.NO_VENDOR_CODE, session, this);
  }

  /**
   * @param queryId the query ID
   * @param session SFBaseSession
   * @param reason exception reason
   */
  public SnowflakeSQLLoggedException(String queryId, SFBaseSession session, String reason) {
    super(queryId, reason, null);
    SqlExceptionTelemetryHandler.sendTelemetry(
        queryId, null, TelemetryUtil.NO_VENDOR_CODE, session, this);
  }
}
