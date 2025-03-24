package net.snowflake.client.core;

import net.snowflake.client.jdbc.OCSPErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class SFOCSPException extends Throwable {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFOCSPException.class);

  private static final long serialVersionUID = 1L;

  private final OCSPErrorCode errorCode;

  public SFOCSPException(OCSPErrorCode errorCode, String errorMsg) {
    this(errorCode, errorMsg, null);
  }

  public SFOCSPException(OCSPErrorCode errorCode, String errorMsg, Throwable cause) {
    super(errorMsg);
    this.errorCode = errorCode;
    if (cause != null) {
      this.initCause(cause);
    }
  }

  public OCSPErrorCode getErrorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    return super.toString()
        + (getErrorCode() != null ? ", errorCode = " + getErrorCode() : "")
        + (getMessage() != null ? ", errorMsg = " + getMessage() : "");
  }
}
