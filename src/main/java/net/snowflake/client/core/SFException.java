package net.snowflake.client.core;

import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.ResourceBundleManager;

public class SFException extends Throwable {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFException.class);

  private static final long serialVersionUID = 1L;

  static final ResourceBundleManager errorResourceBundleManager =
      ResourceBundleManager.getSingleton(ErrorCode.errorMessageResource);

  private Throwable cause;
  private String queryId;
  private String sqlState;
  private int vendorCode;
  private Object[] params;

  /**
   * Use {@link SFException#SFException(String, Throwable, ErrorCode, Object...)}
   *
   * @param errorCode the error code
   * @param params additional params
   */
  @Deprecated
  public SFException(ErrorCode errorCode, Object... params) {
    this(null, null, errorCode, params);
  }

  /**
   * use {@link SFException#SFException(String, Throwable, ErrorCode, Object...)}
   *
   * @param queryID the query id
   * @param errorCode the error code
   * @param params additional params
   */
  @Deprecated
  public SFException(String queryID, ErrorCode errorCode, Object... params) {
    this(queryID, null, errorCode, params);
  }

  /**
   * use {@link SFException#SFException(String, Throwable, ErrorCode, Object...)}
   *
   * @param cause throwable
   * @param errorCode error code
   * @param params additional params
   */
  @Deprecated
  public SFException(Throwable cause, ErrorCode errorCode, Object... params) {
    this(null, cause, errorCode, params);
  }

  /**
   * @param queryId query ID
   * @param cause throwable
   * @param errorCode error code
   * @param params additional params
   */
  public SFException(String queryId, Throwable cause, ErrorCode errorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(errorCode.getMessageCode()), params),
        cause);

    this.cause = null;
    this.queryId = queryId;
    this.sqlState = errorCode.getSqlState();
    this.vendorCode = errorCode.getMessageCode();
    this.params = params;
  }

  /**
   * Get the error cause
   *
   * @return Throwable
   */
  public Throwable getCause() {
    return cause;
  }

  /**
   * Get the query ID
   *
   * @return query ID string
   */
  public String getQueryId() {
    return queryId;
  }

  /**
   * Get the SQL state
   *
   * @return SQL state string
   */
  public String getSqlState() {
    return sqlState;
  }

  /**
   * Get the vendor code
   *
   * @return vendor code
   */
  public int getVendorCode() {
    return vendorCode;
  }

  /**
   * Get additional parameters
   *
   * @return parameter array
   */
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
