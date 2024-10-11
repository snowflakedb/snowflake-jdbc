/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class URLUtilTest {

  @Test
  public void testValidURL() throws Exception {
    Assertions.assertTrue(URLUtil.isValidURL("https://ssoTestURL.okta.com"));
    Assertions.assertTrue(URLUtil.isValidURL("https://ssoTestURL.okta.com:8080"));
    Assertions.assertTrue(URLUtil.isValidURL("https://ssoTestURL.okta.com/testpathvalue"));
  }

  @Test
  public void testInvalidURL() throws Exception {
    Assertions.assertFalse(URLUtil.isValidURL("-a Calculator"));
    Assertions.assertFalse(URLUtil.isValidURL("This is random text"));
    Assertions.assertFalse(URLUtil.isValidURL("file://TestForFile"));
  }

  @Test
  public void testEncodeURL() throws Exception {
    Assertions.assertEquals(URLUtil.urlEncode("Hello @World"), "Hello+%40World");
    Assertions.assertEquals(URLUtil.urlEncode("Test//String"), "Test%2F%2FString");
  }
}
