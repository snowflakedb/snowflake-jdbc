package net.snowflake.client.core;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.UnsupportedEncodingException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;

public class URLUtilTest {

  @Test
  public void testValidURL() throws Exception {
    assertTrue(URLUtil.isValidURL("https://ssoTestURL.okta.com"));
    assertTrue(URLUtil.isValidURL("https://ssoTestURL.okta.com:8080"));
    assertTrue(URLUtil.isValidURL("https://ssoTestURL.okta.com/testpathvalue"));
  }

  @Test
  public void testInvalidURL() throws Exception {
    assertFalse(URLUtil.isValidURL("-a Calculator"));
    assertFalse(URLUtil.isValidURL("This is random text"));
    assertFalse(URLUtil.isValidURL("file://TestForFile"));
  }

  @Test
  public void testEncodeURL() throws Exception {
    assertEquals(URLUtil.urlEncode("Hello @World"), "Hello+%40World");
    assertEquals(URLUtil.urlEncode("Test//String"), "Test%2F%2FString");
  }

  @Test
  void testIsValidURL_InvalidCases() {
    assertFalse(URLUtil.isValidURL("htp://invalid-url"));
    assertFalse(URLUtil.isValidURL("://missing-protocol.com"));
    assertFalse(URLUtil.isValidURL("https:/missing-slash.com"));
    assertFalse(URLUtil.isValidURL("https://"));
    assertFalse(URLUtil.isValidURL(""));
  }

  @ParameterizedTest
  @CsvSource({"'hello world!', hello+world%21", "'', ''"})
  @DisplayName("URL encoding valid and empty strings")
  void testUrlEncode_ValidInputs(String input, String expected)
      throws UnsupportedEncodingException {
    assertEquals(expected, URLUtil.urlEncode(input));
  }

  @ParameterizedTest
  @NullSource
  @DisplayName("URL encoding null should throw NullPointerException")
  void testUrlEncode_NullInput(String input) {
    assertThrows(NullPointerException.class, () -> URLUtil.urlEncode(input));
  }
}
