package net.snowflake.client.log;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class SFLoggerFactoryTest {

  @Test
  public void testGetLoggerByNameDefault() {
    SFLogger sflogger = SFLoggerFactory.getLogger("SnowflakeConnectionV1");
    assertTrue(sflogger instanceof JDK14Logger);
  }
}
