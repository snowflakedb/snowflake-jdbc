package net.snowflake.client.internal.jdbc.cloud.storage;

import org.apache.http.HttpStatus;
import software.amazon.awssdk.core.exception.SdkServiceException;

/**
 * Custom exception class to signal a remote provider exception in a platform-independent manner.
 */
public class StorageProviderException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /**
   * Constructor that accepts an arbitrary Exception.
   *
   * @param ex An Exception to be treated as transient.
   */
  public StorageProviderException(Exception ex) {
    super(ex);
  }

  /**
   * Method to obtain the original provider exception that led to this exception being thrown.
   *
   * @return The original provider exception that led to this exception.
   */
  public Exception getOriginalProviderException() {
    return (Exception) (super.getCause());
  }

  /**
   * Returns true if this is an exception corresponding to a HTTP 404 error returned by the storage
   * provider.
   *
   * @return true if the specified exception is an SdkServiceException instance and if it was thrown
   *     because of a 404, false otherwise.
   */
  public boolean isServiceException404() {
    Throwable cause = getCause();
    if (cause instanceof SdkServiceException) {
      SdkServiceException asEx = (SdkServiceException) cause;
      return (asEx.statusCode() == HttpStatus.SC_NOT_FOUND);
    }

    return false;
  }
}
