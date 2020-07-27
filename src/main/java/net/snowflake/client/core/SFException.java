/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.ResourceBundleManager;

/** Created by jhuang on 1/5/16. */
public class SFException extends Throwable {
  static final SFLogger logger = SFLoggerFactory.getLogger(SFException.class);

  private static final long serialVersionUID = 1L;

  static final ResourceBundleManager errorResourceBundleManager =
      ResourceBundleManager.getSingleton(ErrorCode.errorMessageResource);

  private Throwable cause;
  private String queryId;
  private String sqlState;
  private int vendorCode;
  private Object[] params;

  public SFException(ErrorCode errorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(errorCode.getMessageCode()), params));

    this.cause = null;
    this.queryId = null;
    this.sqlState = errorCode.getSqlState();
    this.vendorCode = errorCode.getMessageCode();
    this.params = params;
  }

  public SFException(Throwable cause, ErrorCode errorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(errorCode.getMessageCode()), params),
        cause);

    this.cause = null;
    this.queryId = null;
    this.sqlState = errorCode.getSqlState();
    this.vendorCode = errorCode.getMessageCode();
    this.params = params;
  }

  public Throwable getCause() {
    return cause;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getSqlState() {
    return sqlState;
  }

  public int getVendorCode() {
    return vendorCode;
  }

  public Object[] getParams() {
    return params;
  }

  @Override
  public String toString() {
    return super.toString()
        + (getQueryId() != null ? ", query id = " + getQueryId() : "")
        + (getSqlState() != null ? ", sql state = " + getSqlState() : "");
  }
}
