package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.AmazonServiceException;
import org.apache.http.HttpStatus;

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
   * provider
   *
   * @return true if the specified exception is an AmazonServiceException instance and if it was
   *     thrown because of a 404, false otherwise.
   */
  public boolean isServiceException404() {
    if ((Exception) this instanceof AmazonServiceException) {
      AmazonServiceException asEx = (AmazonServiceException) ((java.lang.Exception) this);
      return (asEx.getStatusCode() == HttpStatus.SC_NOT_FOUND);
    }

    return false;
  }
}
