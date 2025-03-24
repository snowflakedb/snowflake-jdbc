package net.snowflake.client.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class CommonsLoggingWrapperModeTest {

  private String propertyValue;

  @BeforeEach
  public void setUp() {
    propertyValue = System.getProperty(CommonsLoggingWrapperMode.JAVA_PROPERTY);
  }

  @AfterEach
  public void tearDown() {
    if (propertyValue != null) {
      System.setProperty(CommonsLoggingWrapperMode.JAVA_PROPERTY, propertyValue);
    } else {
      System.clearProperty(CommonsLoggingWrapperMode.JAVA_PROPERTY);
    }
  }

  @ParameterizedTest
  @EnumSource(CommonsLoggingWrapperMode.class)
  public void shouldDetectMode(CommonsLoggingWrapperMode value) {
    System.setProperty(CommonsLoggingWrapperMode.JAVA_PROPERTY, value.name());
    assertEquals(value, CommonsLoggingWrapperMode.detect());
  }

  @Test
  public void shouldPickDefaultMode() {
    System.clearProperty(CommonsLoggingWrapperMode.JAVA_PROPERTY);
    assertEquals(CommonsLoggingWrapperMode.DEFAULT, CommonsLoggingWrapperMode.detect());
  }

  @Test
  public void shouldThrowOnUnknownMode() {
    System.setProperty(CommonsLoggingWrapperMode.JAVA_PROPERTY, "invalid");
    IllegalArgumentException illegalArgumentException =
        assertThrows(IllegalArgumentException.class, CommonsLoggingWrapperMode::detect);
    assertEquals(
        "Unknown commons logging wrapper value 'invalid', expected one of: ALL, DEFAULT, OFF",
        illegalArgumentException.getMessage());
  }
}
