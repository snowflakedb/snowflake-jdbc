package net.snowflake.client.core;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.junit.jupiter.api.Test;

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
    assertFalse(URLUtil.isValidURL(null));
  }

  @Test
  void testUrlEncode_ValidString() throws UnsupportedEncodingException {
    String input = "hello world!";
    String expected = "hello+world%21"; // URLEncoder replaces spaces with '+'
    assertEquals(expected, URLUtil.urlEncode(input));
  }

  @Test
  void testUrlEncode_EmptyString() throws UnsupportedEncodingException {
    assertEquals("", URLUtil.urlEncode(""));
  }

  @Test
  void testIsValidURL_PatternSyntaxException()
      throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
          SecurityException {
    // Access the static final pattern field
    Field patternField = URLUtil.class.getDeclaredField("pattern");
    patternField.setAccessible(true);

    // Remove final modifier using reflection
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(patternField, patternField.getModifiers() & ~Modifier.FINAL);

    // Save the original pattern
    Pattern originalPattern = (Pattern) patternField.get(null);

    try {
      // Mock a broken pattern that throws PatternSyntaxException
      Pattern mockPattern = mock(Pattern.class);
      when(mockPattern.matcher(anyString()))
          .thenThrow(new PatternSyntaxException("Invalid regex", ".*", -1));

      // Set the modified mock pattern
      patternField.set(null, mockPattern);

      // Call the method and verify it handles the exception gracefully
      assertTrue(URLUtil.isValidURL("https://valid.url.com")); // Falls back to basic URL check
    } finally {
      // Restore the original pattern to avoid affecting other tests
      patternField.set(null, originalPattern);
    }
  }

  @Test
  void testUrlEncode_NullString() {
    assertThrows(NullPointerException.class, () -> URLUtil.urlEncode(null));
  }
}
