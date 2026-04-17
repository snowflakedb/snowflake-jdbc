package net.snowflake.client.internal.core;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.util.HashMap;
import net.snowflake.client.api.exception.ErrorCode;
import org.junit.jupiter.api.Test;

public class SFSessionPropertyTest {
  @Test
  public void testCheckApplicationName() throws SFException {
    String[] validApplicationName = {"test1234", "test_1234", "test-1234", "test.1234"};

    String[] invalidApplicationName = {"1234test", "test$A", "test<script>"};

    for (String valid : validApplicationName) {
      Object value = SFSessionProperty.checkPropertyValue(SFSessionProperty.APPLICATION, valid);

      assertThat((String) value, is(valid));
    }

    for (String invalid : invalidApplicationName) {
      SFException e =
          assertThrows(
              SFException.class,
              () -> {
                SFSessionProperty.checkPropertyValue(SFSessionProperty.APPLICATION, invalid);
              });
      assertThat(e.getVendorCode(), is(ErrorCode.INVALID_PARAMETER_VALUE.getMessageCode()));
    }
  }

  @Test
  public void testCustomSuffixForUserAgentHeaders() {
    String customSuffix = "test-suffix";
    String userAgentHeader = HttpUtil.buildUserAgent(customSuffix);

    assertThat(
        "user-agent header should contain the suffix ", userAgentHeader, endsWith(customSuffix));
  }

  @Test
  public void testInvalidMaxRetries() {
    SFException e =
        assertThrows(
            SFException.class,
            () -> {
              SFSessionProperty.checkPropertyValue(
                  SFSessionProperty.MAX_HTTP_RETRIES, "invalidValue");
            });
    assertThat(e.getVendorCode(), is(ErrorCode.INVALID_PARAMETER_VALUE.getMessageCode()));
  }

  @Test
  public void testvalidMaxRetries() throws SFException {
    int expectedVal = 10;
    Object value =
        SFSessionProperty.checkPropertyValue(SFSessionProperty.MAX_HTTP_RETRIES, expectedVal);

    assertThat("Integer value should match", (int) value == expectedVal);
  }

  @Test
  public void testInvalidPutGetMaxRetries() {
    SFException e =
        assertThrows(
            SFException.class,
            () -> {
              SFSessionProperty.checkPropertyValue(
                  SFSessionProperty.PUT_GET_MAX_RETRIES, "invalidValue");
            });
    assertThat(e.getVendorCode(), is(ErrorCode.INVALID_PARAMETER_VALUE.getMessageCode()));
  }

  @Test
  public void testvalidPutGetMaxRetries() throws SFException {
    int expectedVal = 10;
    Object value =
        SFSessionProperty.checkPropertyValue(SFSessionProperty.PUT_GET_MAX_RETRIES, expectedVal);

    assertThat("Integer value should match", (int) value == expectedVal);
  }

  @Test
  public void testEnableCopyResultSetPropertyRegistered() {
    SFSessionProperty prop = SFSessionProperty.lookupByKey("enableCopyResultSet");
    assertNotNull(prop);
    assertEquals(SFSessionProperty.ENABLE_COPY_RESULT_SET, prop);
    assertEquals(Boolean.class, prop.getValueType());
  }

  @Test
  void testEnableCopyResultSetDefaultFalse() {
    SFBaseSession session = mock(SFBaseSession.class, CALLS_REAL_METHODS);
    assertFalse(session.isEnableCopyResultSet(), "default must be false for backwards compatibility");
  }

  @Test
  void testEnableCopyResultSetCanBeSetTrue() {
    SFBaseSession session = mock(SFBaseSession.class, CALLS_REAL_METHODS);
    session.setEnableCopyResultSet(true);
    assertTrue(session.isEnableCopyResultSet());
  }

  @Test
  void testEnableCopyResultSetCanBeReset() {
    SFBaseSession session = mock(SFBaseSession.class, CALLS_REAL_METHODS);
    session.setEnableCopyResultSet(true);
    session.setEnableCopyResultSet(false);
    assertFalse(session.isEnableCopyResultSet(), "flag must be resettable to false");
  }

  @Test
  void testAddSFSessionPropertyWiresEnableCopyResultSet() throws SFException, ReflectiveOperationException {
    SFSession session = mock(SFSession.class, CALLS_REAL_METHODS);
    Field mapField = SFBaseSession.class.getDeclaredField("connectionPropertiesMap");
    mapField.setAccessible(true);
    mapField.set(session, new HashMap<>());
    session.addSFSessionProperty(
        SFSessionProperty.ENABLE_COPY_RESULT_SET.getPropertyKey(), Boolean.TRUE);
    assertTrue(session.isEnableCopyResultSet());
  }
}
