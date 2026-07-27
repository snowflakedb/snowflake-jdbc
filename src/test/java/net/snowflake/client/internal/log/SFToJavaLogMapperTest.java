package net.snowflake.client.internal.log;

import static net.snowflake.client.internal.log.SFToJavaLogMapper.toJavaUtilLoggingLevel;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.logging.Level;
import org.junit.jupiter.api.Test;

public class SFToJavaLogMapperTest {

  @Test
  public void testToJavaUtilLoggingLevel() {
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.OFF), Level.OFF);
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.ERROR), Level.SEVERE);
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.WARN), Level.WARNING);
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.INFO), Level.INFO);
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.DEBUG), Level.FINE);
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.TRACE), Level.FINEST);
  }
}
