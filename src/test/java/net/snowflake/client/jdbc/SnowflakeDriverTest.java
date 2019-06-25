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
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081?a=b"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081/?a=b"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081?a=b&c=d"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8081/?a=b&c=d"));

    assertTrue(snowflakeDriver.acceptsURL(
        "jdbc:snowflake://localhost:8082/?account=TESTACCOUNT&" +
        "user=qa@snowflakecomputing.com&ssl=off&schema=testschema&db=testdb"));
    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://snowflake.reg.local:8082/?account=TESTACCOUNT&" +
    "user=qa@snowflakecomputing.com&ssl=off&schema=testschema&db=testdb&networkTimeout=3600"));

    assertTrue(snowflakeDriver.acceptsURL("jdbc:snowflake://snowflake.reg.local:8082/?account=testaccount&authenticator=https://snowflakecomputing.okta.com/app/template_saml_2_0/ky7gy61iAOAMLLSOZSVX/sso/saml&user=qa&ssl=off&schema=testschema&db=testdb&networkTimeout=3600&retryQuery=on"));
    // negative tests
    assertFalse(snowflakeDriver.acceptsURL("jdbc:"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://:"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://:8080"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:xyz"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflak://localhost:8080"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8080/a=b"));
  }
}
