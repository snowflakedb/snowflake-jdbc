/*
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import net.snowflake.client.jdbc.OCSPErrorCode;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class SFOCSPException extends Throwable
{
  static final SFLogger logger = SFLoggerFactory.getLogger(SFOCSPException.class);

  private static final long serialVersionUID = 1L;

  private String errorMsg;
  private OCSPErrorCode errorCode;

  public SFOCSPException(OCSPErrorCode errorCode,
                         String errorMsg)
  {

    this.errorMsg = errorMsg;
    this.errorCode = errorCode;
  }

  public OCSPErrorCode getErrorCode()
  {
    return errorCode;
  }

  public String getErrorMsg()
  {
    return errorMsg;
  }

  @Override
  public String toString()
  {
    return super.toString() +
           (getErrorCode() != null ? ", errorCode = " + getErrorCode() : "") +
           (getErrorMsg() != null ? ", errorMsg = " + getErrorMsg() : "");
  }
}
