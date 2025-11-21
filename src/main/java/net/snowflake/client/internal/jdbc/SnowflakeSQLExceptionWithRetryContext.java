package net.snowflake.client.internal.jdbc;

import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;

/**
 * Internal exception class that extends SnowflakeSQLException with additional retry context
 * information. This class is used internally by the JDBC driver for retry logic and should not be
 * exposed to customers.
 *
 * <p>This exception carries metadata about retry attempts, timeouts, and elapsed time that is used
 * by internal components (RestRequest, SessionUtil) to manage connection and authentication retry
 * logic.
 */
public class SnowflakeSQLExceptionWithRetryContext extends SnowflakeSQLException {
  private static final long serialVersionUID = 1L;

  private final int retryCount;
  private final boolean isSocketTimeoutNoBackoff;
  private final long elapsedSeconds;

  /**
   * Constructs a new exception with retry context information.
   *
   * @param errorCode the error code
   * @param retryCount the number of retry attempts made
   * @param isSocketTimeoutNoBackoff whether the socket timeout occurred without backoff
   * @param elapsedSeconds the elapsed time in seconds
   */
  public SnowflakeSQLExceptionWithRetryContext(
      ErrorCode errorCode, int retryCount, boolean isSocketTimeoutNoBackoff, long elapsedSeconds) {
    super(errorCode);
    this.retryCount = retryCount;
    this.isSocketTimeoutNoBackoff = isSocketTimeoutNoBackoff;
    this.elapsedSeconds = elapsedSeconds;
  }

  /**
   * Gets the retry count for this exception.
   *
   * @return the number of retry attempts
   */
  public int getRetryCount() {
    return retryCount;
  }

  /**
   * Checks if the socket timeout occurred without backoff.
   *
   * @return true if socket timeout had no backoff, false otherwise
   */
  public boolean isSocketTimeoutNoBackoff() {
    return isSocketTimeoutNoBackoff;
  }

  /**
   * Gets the elapsed time in seconds.
   *
   * @return elapsed seconds
   */
  public long getElapsedSeconds() {
    return elapsedSeconds;
  }
}
