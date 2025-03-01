package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
