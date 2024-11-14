package net.snowflake.client.log;

/** Extended log levels for snowflake package. */
public enum SFLogLevel {
  // OFF is highest level, no logs will be shown at this level.
  // We can extend this enum to add levels for perf instrumentation and network request/response.
  OFF(50, "OFF"),
  ERROR(40, "ERROR"),
  WARN(30, "WARN"),
  INFO(20, "INFO"),
  DEBUG(10, "DEBUG"),
  TRACE(0, "TRACE");

  private final int levelInt;
  private final String levelStr;

  SFLogLevel(int levelInt, String levelStr) {
    this.levelInt = levelInt;
    this.levelStr = levelStr;
  }

  /**
   * Method to parse the input loglevel string and returns corresponding loglevel. This method uses
   * case in-sensitive matching.
   *
   * @param levelStr log level string
   * @return SFLogLevel
   */
  public static SFLogLevel getLogLevel(String levelStr) {
    for (SFLogLevel level : SFLogLevel.values()) {
      if (level.levelStr.equalsIgnoreCase(levelStr)) {
        return level;
      }
    }

    // Default is off.
    return SFLogLevel.OFF;
  }
}
