/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import net.snowflake.client.jdbc.ErrorCode;
import org.junit.Assert;
import org.junit.Test;

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
      try {
        SFSessionProperty.checkPropertyValue(SFSessionProperty.APPLICATION, invalid);
        Assert.fail();
      } catch (SFException e) {
        assertThat(e.getVendorCode(), is(ErrorCode.INVALID_PARAMETER_VALUE.getMessageCode()));
      }
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
    try {
      SFSessionProperty.checkPropertyValue(SFSessionProperty.MAX_HTTP_RETRIES, "invalidValue");
      Assert.fail("testInvalidMaxRetries");
    } catch (SFException e) {
      assertThat(e.getVendorCode(), is(ErrorCode.INVALID_PARAMETER_VALUE.getMessageCode()));
    }
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
    try {
      SFSessionProperty.checkPropertyValue(SFSessionProperty.PUT_GET_MAX_RETRIES, "invalidValue");
      Assert.fail("testInvalidMaxRetries");
    } catch (SFException e) {
      assertThat(e.getVendorCode(), is(ErrorCode.INVALID_PARAMETER_VALUE.getMessageCode()));
    }
  }

  @Test
  public void testvalidPutGetMaxRetries() throws SFException {
    int expectedVal = 10;
    Object value =
        SFSessionProperty.checkPropertyValue(SFSessionProperty.PUT_GET_MAX_RETRIES, expectedVal);

    assertThat("Integer value should match", (int) value == expectedVal);
  }
}
