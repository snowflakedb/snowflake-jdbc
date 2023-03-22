/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SFLoggerFactoryTest {

  @Test
  public void testGetLoggerByNameDefault() {
    SFLogger sflogger = SFLoggerFactory.getLogger("SnowflakeConnectionV1");
    assertTrue(sflogger instanceof JDK14Logger);
  }
}
