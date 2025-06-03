package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonServiceException;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

/** Tests for test coverage of StorageProviderException class. */
public class StorageProviderExceptionTest {

  /** Test should construct original provider exception. */
  @Test
  void testStorageProviderExceptionConstructor() {
    Exception originalException = new Exception("Original exception");
    StorageProviderException exception = new StorageProviderException(originalException);
    assertNotNull(exception);
  }

  /** Test should return original provider exception. */
  @Test
  void testStorageProviderExceptionShouldReturnOriginalException() {
    Exception originalException = new Exception("Original exception");
    StorageProviderException exception = new StorageProviderException(originalException);
    assertEquals(originalException, exception.getOriginalProviderException());
  }

  /**
   * Test when the cause is an AmazonServiceException with a 404 status code. This should return
   * true since the status code matches HTTP 404.
   */
  @Test
  void testStorageProviderExceptionWhenAmazonServiceException404() {
    AmazonServiceException mockException = mock(AmazonServiceException.class);
    when(mockException.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    StorageProviderException exception = new StorageProviderException(mockException);
    // Assert: check if the method returns true when status code is 404
    assertTrue(exception.isServiceException404());
  }

  /**
   * Test when the cause is an AmazonServiceException with a status code other than 404. This should
   * return false since the status code does not match HTTP 404.
   */
  @Test
  void testStorageProviderExceptionWhenAmazonServiceExceptionNot404() {
    AmazonServiceException mockException = mock(AmazonServiceException.class);
    when(mockException.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    StorageProviderException exception = new StorageProviderException(mockException);
    // Assert: check if the method returns false when status code is not 404
    assertFalse(exception.isServiceException404());
  }

  /**
   * Test when the cause is not an AmazonServiceException (e.g., a general Exception). This should
   * return false since the cause is not an instance of AmazonServiceException.
   */
  @Test
  void testStorageProviderExceptionWhenNotAmazonServiceException() {
    Exception mockException = mock(Exception.class);
    StorageProviderException exception = new StorageProviderException(mockException);
    // Assert: check if the method returns false when the exception is not AmazonServiceException
    assertFalse(exception.isServiceException404());
  }
}
