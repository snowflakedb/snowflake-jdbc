package net.snowflake.client.log;

import java.util.HashMap;
import java.util.logging.Level;

/** Utility class to map SFLogLevels to java.util.logging.Level; */
public class SFToJavaLogMapper {
  private static HashMap<SFLogLevel, Level> levelMap = new HashMap<>();

  static {
    levelMap.put(SFLogLevel.TRACE, java.util.logging.Level.FINEST);
    levelMap.put(SFLogLevel.DEBUG, java.util.logging.Level.FINE);
    levelMap.put(SFLogLevel.INFO, java.util.logging.Level.INFO);
    levelMap.put(SFLogLevel.WARN, java.util.logging.Level.WARNING);
    levelMap.put(SFLogLevel.ERROR, java.util.logging.Level.SEVERE);
    levelMap.put(SFLogLevel.OFF, java.util.logging.Level.OFF);
  }

  public static java.util.logging.Level toJavaUtilLoggingLevel(SFLogLevel level) {
    return levelMap.getOrDefault(level, java.util.logging.Level.OFF);
  }
}
