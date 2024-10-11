package net.snowflake.client.log;

import static net.snowflake.client.log.SFToJavaLogMapper.toJavaUtilLoggingLevel;

import java.util.logging.Level;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SFToJavaLogMapperTest {

  @Test
  public void testToJavaUtilLoggingLevel() {
    Assertions.assertEquals(toJavaUtilLoggingLevel(SFLogLevel.OFF), Level.OFF);
    Assertions.assertEquals(toJavaUtilLoggingLevel(SFLogLevel.ERROR), Level.SEVERE);
    Assertions.assertEquals(toJavaUtilLoggingLevel(SFLogLevel.WARN), Level.WARNING);
    Assertions.assertEquals(toJavaUtilLoggingLevel(SFLogLevel.INFO), Level.INFO);
    Assertions.assertEquals(toJavaUtilLoggingLevel(SFLogLevel.DEBUG), Level.FINE);
    Assertions.assertEquals(toJavaUtilLoggingLevel(SFLogLevel.TRACE), Level.FINEST);
  }
}
