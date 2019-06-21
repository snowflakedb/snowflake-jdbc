/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Driver unit test
 */
public class SnowflakeDriverTest
{
  @Test
  public void testAcceptUrls() throws Exception
  {
    SnowflakeDriver snowflakeDriver = SnowflakeDriver.INSTANCE;
    // positive tests
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost8081?a=b"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081/?a=b"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081?a=b&c=d"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081/?a=b&c=d"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://testaccount.snowflakecomputing" +
                                          ".com:443?CLIENT_SESSION_KEEP_ALIVE=true&db=TEST_DB&proxyHost=your-host" +
                                          ".com&proxyPort=1234&schema=PUBLIC&useProxy=true&warehouse=TEST_WH"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://testaccount.snowflakecomputing.com?proxyHost=72.1.32" +
                                          ".55&proxyPort=port%55"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8080?proxyHost=%3d%2f&proxyPort=777"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8080?proxyHost=cheese&proxyPort=!@"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://http://testaccount.localhost?prop1=value1"));
    // negative tests
    assertFalse(snowflakeDriver.acceptsURL("jdbc:"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://:"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://:8080"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:xyz"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflak://localhost:8080"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8080/a=b"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://testaccount.com?proxyHost=%%"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://testaccount.com?proxyHost=puppy==dog&proxyPort=781"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://testaccount.com?proxyHost=&&&proxyPort=5"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://testaccount.com?proxyHost=%b&proxyPort="));
  }
}
