package net.snowflake.client.log;

import static org.junit.Assert.assertNotNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A test class created for test case coverage for the class CommonsLoggingWrapper. */
class CommonsLoggingWrapperTest {
  private CommonsLoggingWrapper logWrapper;

  @BeforeEach
  void setUp() {
    logWrapper = new CommonsLoggingWrapper("test.class");
  }

  @Test
  void testDebug() {
    String message = "Debug Message";

    // Call the method
    logWrapper.debug(message);
    assertNotNull(logWrapper);
  }

  @Test
  void testDebugWithThrowable() {
    String message = "Debug Message with Throwable";
    Throwable throwable = new Throwable("Throwable");

    // Call the method
    logWrapper.debug(message, throwable);
    assertNotNull(logWrapper);
  }

  @Test
  void testError() {
    String message = "Error Message";

    // Call the method
    logWrapper.error(message);
    assertNotNull(logWrapper);
  }

  @Test
  void testErrorWithThrowable() {
    String message = "Error Message with Throwable";
    Throwable throwable = new Throwable("Throwable");

    // Call the method
    logWrapper.error(message, throwable);
    assertNotNull(logWrapper);
  }

  @Test
  void testFatal() {
    String message = "Fatal Message";

    // Call the method
    logWrapper.fatal(message);
    assertNotNull(logWrapper);
  }

  @Test
  void testFatalWithThrowable() {
    String message = "Fatal Message with Throwable";
    Throwable throwable = new Throwable("Throwable");

    // Call the method
    logWrapper.fatal(message, throwable);
    assertNotNull(logWrapper);
  }

  @Test
  void testInfo() {
    String message = "Info Message";

    // Call the method
    logWrapper.info(message);
    assertNotNull(logWrapper);
  }

  @Test
  void testInfoWithThrowable() {
    String message = "Info Message with Throwable";
    Throwable throwable = new Throwable("Throwable");

    logWrapper.info(message, throwable);
    assertNotNull(logWrapper);
  }

  @Test
  void testIsDebugEnabled() {

    logWrapper.isDebugEnabled();
    assertNotNull(logWrapper);
  }

  @Test
  void testIsErrorEnabled() {
    // Call the method
    logWrapper.isErrorEnabled();
    assertNotNull(logWrapper);
  }

  @Test
  void testIsFatalEnabled() {
    logWrapper.isFatalEnabled();
    assertNotNull(logWrapper);
  }

  @Test
  void testIsInfoEnabled() {

    // Call the method
    logWrapper.isInfoEnabled();
    assertNotNull(logWrapper);
  }

  @Test
  void testIsTraceEnabled() {

    logWrapper.isTraceEnabled();
    assertNotNull(logWrapper);
  }

  @Test
  void testIsWarnEnabled() {

    // Call the method
    logWrapper.isWarnEnabled();
    assertNotNull(logWrapper);
  }

  @Test
  void testTrace() {
    String message = "Trace Message";

    // Call the method
    logWrapper.trace(message);
    assertNotNull(logWrapper);
  }

  @Test
  void testTraceWithThrowable() {
    String message = "Trace Message with Throwable";
    Throwable throwable = new Throwable("Throwable");

    // Call the method
    logWrapper.trace(message, throwable);
    assertNotNull(logWrapper);
  }

  @Test
  void testWarn() {
    String message = "Warn Message";

    // Call the method
    logWrapper.warn(message);
    assertNotNull(logWrapper);
  }

  @Test
  void testWarnWithThrowable() {
    String message = "Warn Message with Throwable";
    Throwable throwable = new Throwable("Throwable");

    // Call the method
    logWrapper.warn(message, throwable);
    assertNotNull(logWrapper);
  }
}
