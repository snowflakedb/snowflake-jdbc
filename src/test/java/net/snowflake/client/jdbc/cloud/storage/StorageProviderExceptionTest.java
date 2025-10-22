package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkServiceException;

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
   * Test when the cause is an SdkServiceException with a 404 status code. This should return true
   * since the status code matches HTTP 404.
   */
  @Test
  void testStorageProviderExceptionWhenSdkServiceException404() {
    SdkServiceException mockException = mock(SdkServiceException.class);
    when(mockException.statusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    StorageProviderException exception = new StorageProviderException(mockException);
    // Assert: check if the method returns true when status code is 404
    assertTrue(exception.isServiceException404());
  }

  /**
   * Test when the cause is an SdkServiceException with a status code other than 404. This should
   * return false since the status code does not match HTTP 404.
   */
  @Test
  void testStorageProviderExceptionWhenSdkServiceExceptionNot404() {
    SdkServiceException mockException = mock(SdkServiceException.class);
    when(mockException.statusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    StorageProviderException exception = new StorageProviderException(mockException);
    // Assert: check if the method returns false when status code is not 404
    assertFalse(exception.isServiceException404());
  }

  /**
   * Test when the cause is not an SdkServiceException (e.g., a general Exception). This should
   * return false since the cause is not an instance of SdkServiceException.
   */
  @Test
  void testStorageProviderExceptionWhenNotSdkServiceException() {
    Exception mockException = mock(Exception.class);
    StorageProviderException exception = new StorageProviderException(mockException);
    // Assert: check if the method returns false when the exception is not SdkServiceException
    assertFalse(exception.isServiceException404());
  }
}
