package net.snowflake.client.log;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SFLogLevelTest {

  @Test
  public void testFromString() {
    Assertions.assertTrue(SFLogLevel.OFF.equals(SFLogLevel.getLogLevel("Off")));
    Assertions.assertTrue(SFLogLevel.OFF.equals(SFLogLevel.getLogLevel("OFF")));
    Assertions.assertTrue(SFLogLevel.ERROR.equals(SFLogLevel.getLogLevel("ERROR")));
    Assertions.assertTrue(SFLogLevel.WARN.equals(SFLogLevel.getLogLevel("WARN")));
    Assertions.assertTrue(SFLogLevel.INFO.equals(SFLogLevel.getLogLevel("INFO")));
    Assertions.assertTrue(SFLogLevel.DEBUG.equals(SFLogLevel.getLogLevel("DEBUG")));
    Assertions.assertTrue(SFLogLevel.TRACE.equals(SFLogLevel.getLogLevel("TRACE")));
  }
}
