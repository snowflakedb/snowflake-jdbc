package net.snowflake.client.log;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class SFLogLevelTest {

  @Test
  public void testFromString() {
    assertTrue(SFLogLevel.OFF.equals(SFLogLevel.getLogLevel("Off")));
    assertTrue(SFLogLevel.OFF.equals(SFLogLevel.getLogLevel("OFF")));
    assertTrue(SFLogLevel.ERROR.equals(SFLogLevel.getLogLevel("ERROR")));
    assertTrue(SFLogLevel.WARN.equals(SFLogLevel.getLogLevel("WARN")));
    assertTrue(SFLogLevel.INFO.equals(SFLogLevel.getLogLevel("INFO")));
    assertTrue(SFLogLevel.DEBUG.equals(SFLogLevel.getLogLevel("DEBUG")));
    assertTrue(SFLogLevel.TRACE.equals(SFLogLevel.getLogLevel("TRACE")));
  }
}
