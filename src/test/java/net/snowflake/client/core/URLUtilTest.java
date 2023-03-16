/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import static org.junit.Assert.*;

import org.junit.Test;

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
  }

  @Test
  public void testEncodeURL() throws Exception {
    assertEquals(URLUtil.urlEncode("Hello @World"), "Hello+%40World");
    assertEquals(URLUtil.urlEncode("Test//String"), "Test%2F%2FString");
  }
}
