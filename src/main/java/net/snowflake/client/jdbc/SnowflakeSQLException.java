/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.SQLException;
import net.snowflake.client.core.SFException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.ResourceBundleManager;

/** @author jhuang */
public class SnowflakeSQLException extends SQLException {
  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeSQLException.class);

  private static final long serialVersionUID = 1L;

  static final ResourceBundleManager errorResourceBundleManager =
      ResourceBundleManager.getSingleton(ErrorCode.errorMessageResource);

  private String queryId = "unknown";
  private int retryCount = 0;

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
        "Snowflake exception: {}, sqlState:{}, vendorCode:{}, queryId:{}",
        reason,
        sqlState,
        vendorCode,
        queryId);
  }

  public SnowflakeSQLException(String reason, String SQLState) {
    super(reason, SQLState);
    // log user error from GS at fine level
    logger.debug("Snowflake exception: {}, sqlState:{}", reason, SQLState);
  }

  public SnowflakeSQLException(String sqlState, int vendorCode) {
    super(
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode)),
        sqlState,
        vendorCode);

    logger.debug(
        "Snowflake exception: {}, sqlState:{}, vendorCode:{}",
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode)),
        sqlState,
        vendorCode);
  }

  public SnowflakeSQLException(String sqlState, int vendorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params),
        sqlState,
        vendorCode);

    logger.debug(
        "Snowflake exception: {}, sqlState:{}, vendorCode:{}",
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params),
        sqlState,
        vendorCode);
  }

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

  public SnowflakeSQLException(Throwable ex, ErrorCode errorCode, Object... params) {
    this(ex, errorCode.getSqlState(), errorCode.getMessageCode(), params);
  }

  public SnowflakeSQLException(Throwable ex, String sqlState, int vendorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params),
        sqlState,
        vendorCode,
        ex);

    logger.debug(
        "Snowflake exception: "
            + errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params),
        ex);
  }

  public SnowflakeSQLException(ErrorCode errorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(errorCode.getMessageCode()), params),
        errorCode.getSqlState(),
        errorCode.getMessageCode());
  }

  public SnowflakeSQLException(ErrorCode errorCode, int retryCount, String reason) {
    super(
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(errorCode.getMessageCode()), reason),
        errorCode.getSqlState(),
        errorCode.getMessageCode());
    this.retryCount = retryCount;
  }

  public SnowflakeSQLException(SFException e) {
    this(e.getQueryId(), e.getMessage(), e.getSqlState(), e.getVendorCode());
  }

  public SnowflakeSQLException(String reason) {
    super(reason);
  }

  public String getQueryId() {
    return queryId;
  }

  public int getRetryCount() {
    return retryCount;
  }
}
