/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

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
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081?a=b"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081/?a=b"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081?a=b&c=d"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081/?a=b&c=d"));

    // negative tests
    assertTrue(!snowflakeDriver.acceptsURL("jdbc:snowflake://:"));
    assertTrue(!snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:"));
    assertTrue(!snowflakeDriver.acceptsURL("jdbc:snowflake://:8080"));
    assertTrue(!snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:xyz"));
    assertTrue(!snowflakeDriver.acceptsURL("jdbc:snowflak://localhost:8080"));
  }
}
