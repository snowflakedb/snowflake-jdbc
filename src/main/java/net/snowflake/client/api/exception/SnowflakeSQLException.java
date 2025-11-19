package net.snowflake.client.api.exception;

import java.sql.SQLException;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import net.snowflake.common.core.ResourceBundleManager;

public class SnowflakeSQLException extends SQLException {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeSQLException.class);

  private static final long serialVersionUID = 1L;

  static final ResourceBundleManager errorResourceBundleManager =
      ResourceBundleManager.getSingleton(ErrorCode.errorMessageResource);

  private String queryId = "unknown";

  /**
   * This constructor should only be used for error from Global service. Since Global service has
   * already built the error message, we use it as is. For any errors local to JDBC driver, we
   * should use one of the constructors below to build the error message.
   *
   * @param queryId query id
   * @param reason reason for which exception is created
   * @param sqlState sql state
   * @param vendorCode vendor code
   */
  public SnowflakeSQLException(String queryId, String reason, String sqlState, int vendorCode) {
    super(reason, sqlState, vendorCode);

    this.queryId = queryId;

    // log user error from GS at fine level
    logger.debug(
        "Snowflake exception: {}, sqlState: {}, vendorCode: {}, queryId: {}",
        reason,
        sqlState,
        vendorCode,
        queryId);
  }

  /**
   * @param queryId the queryID
   * @param reason exception reason
   * @param sqlState the SQL state
   */
  public SnowflakeSQLException(String queryId, String reason, String sqlState) {
    super(reason, sqlState);
    this.queryId = queryId;
    // log user error from GS at fine level
    logger.debug("Snowflake exception: {}, sqlState:{}", reason, sqlState);
  }

  /**
   * @param queryId query ID
   * @param sqlState SQL state
   * @param vendorCode vendor code
   */
  public SnowflakeSQLException(String queryId, String sqlState, int vendorCode) {
    super(
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode)),
        sqlState,
        vendorCode);
    this.queryId = queryId;
    logger.debug(
        "Snowflake exception: {}, sqlState: {}, vendorCode: {}",
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode)),
        sqlState,
        vendorCode);
  }

  /**
   * @param sqlState the SQL state
   * @param vendorCode the vendor code
   * @param params additional parameters
   */
  public SnowflakeSQLException(String sqlState, int vendorCode, Object... params) {
    this((String) null, sqlState, vendorCode, params);
  }

  /**
   * @param queryId query ID
   * @param sqlState the SQL state
   * @param vendorCode the vendor code
   * @param params additional parameters
   */
  public SnowflakeSQLException(String queryId, String sqlState, int vendorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params),
        sqlState,
        vendorCode);
    this.queryId = queryId;
    logger.debug(
        "Snowflake exception: {}, sqlState: {}, vendorCode: {}",
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params),
        sqlState,
        vendorCode);
  }

  /**
   * @param ex Throwable exception
   * @param sqlState the SQL state
   * @param vendorCode the vendor code
   */
  public SnowflakeSQLException(Throwable ex, String sqlState, int vendorCode) {
    super(
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode)),
        sqlState,
        vendorCode,
        ex);

    logger.debug(
        "Snowflake exception: {}"
            + errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode)),
        ex);
  }

  /**
   * @param ex Throwable exception
   * @param errorCode the error code
   * @param params additional parameters
   */
  public SnowflakeSQLException(Throwable ex, ErrorCode errorCode, Object... params) {
    this(ex, errorCode.getSqlState(), errorCode.getMessageCode(), params);
  }

  /**
   * @param ex Throwable exception
   * @param sqlState the SQL state
   * @param vendorCode the vendor code
   * @param params additional parameters
   */
  public SnowflakeSQLException(Throwable ex, String sqlState, int vendorCode, Object... params) {
    this(null, ex, sqlState, vendorCode, params);
  }

  /**
   * @param queryId query ID
   * @param ex Throwable exception
   * @param sqlState the SQL state
   * @param vendorCode the vendor code
   * @param params additional parameters
   */
  public SnowflakeSQLException(
      String queryId, Throwable ex, String sqlState, int vendorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params),
        sqlState,
        vendorCode,
        ex);
    this.queryId = queryId;

    logger.debug(
        "Snowflake exception: "
            + errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params),
        ex);
  }

  /**
   * @param errorCode the error code
   * @param params additional parameters
   */
  public SnowflakeSQLException(ErrorCode errorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(errorCode.getMessageCode()), params),
        errorCode.getSqlState(),
        errorCode.getMessageCode());
  }

  /**
   * @param queryId query ID
   * @param errorCode error code
   * @param params additional parameters
   */
  public SnowflakeSQLException(String queryId, ErrorCode errorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(errorCode.getMessageCode()), params),
        errorCode.getSqlState(),
        errorCode.getMessageCode());
    this.queryId = queryId;
  }

  /**
   * @param e the SFException
   */
  public SnowflakeSQLException(SFException e) {
    this(e.getQueryId(), e.getMessage(), e.getSqlState(), e.getVendorCode());
  }

  public SnowflakeSQLException(String reason) {
    super(reason);
  }

  public SnowflakeSQLException(Throwable ex, String message) {
    super(message, ex);
  }

  public String getQueryId() {
    return queryId;
  }
}
