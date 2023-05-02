package net.snowflake.client.log;

import static net.snowflake.client.log.SFToJavaLogMapper.toJavaUtilLoggingLevel;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class SFToJavaLogMapperTest {

  @Test
  public void testToJavaUtilLoggingLevel() {
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.OFF), java.util.logging.Level.OFF);
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.ERROR), java.util.logging.Level.SEVERE);
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.WARN), java.util.logging.Level.WARNING);
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.INFO), java.util.logging.Level.INFO);
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.DEBUG), java.util.logging.Level.FINER);
    assertEquals(toJavaUtilLoggingLevel(SFLogLevel.TRACE), java.util.logging.Level.FINEST);
  }
}
