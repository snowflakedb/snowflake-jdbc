package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

/** Used to create SFLogger instance */
public class SFLoggerFactory {
  private static LoggerImpl loggerImplementation;

  enum LoggerImpl {
    SLF4JLOGGER("net.snowflake.client.log.SLF4JLogger"),
    JDK14LOGGER("net.snowflake.client.log.JDK14Logger");

    private String loggerImplClassName;

    LoggerImpl(String loggerClass) {
      this.loggerImplClassName = loggerClass;
    }

    public String getLoggerImplClassName() {
      return this.loggerImplClassName;
    }

    public static LoggerImpl fromString(String loggerImplClassName) {
      if (loggerImplClassName != null) {
        for (LoggerImpl imp : LoggerImpl.values()) {
          if (loggerImplClassName.equalsIgnoreCase(imp.getLoggerImplClassName())) {
            return imp;
          }
        }
      }
      return null;
    }
  }

  /**
   * @param clazz Class type that the logger is instantiated
   * @return An SFLogger instance given the name of the class
   */
  public static SFLogger getLogger(Class<?> clazz) {
    // only need to determine the logger implementation only once
    if (loggerImplementation == null) {
      String logger = systemGetProperty("net.snowflake.jdbc.loggerImpl");

      loggerImplementation = LoggerImpl.fromString(logger);

      if (loggerImplementation == null) {
        // default to use java util logging
        loggerImplementation = LoggerImpl.JDK14LOGGER;
      }
    }

    switch (loggerImplementation) {
      case SLF4JLOGGER:
        return new SLF4JLogger(clazz);
      case JDK14LOGGER:
      default:
        return new JDK14Logger(clazz.getName());
    }
  }

  /**
   * A replacement for getLogger function, whose parameter is Class&lt;?&gt;, when Class&lt;?&gt; is
   * inaccessible. For example, the name we have is an alias name of a class, we can't get the
   * correct Class&lt;?&gt; by the given name.
   *
   * @param name name to indicate the class (might be different with the class name) that the logger
   *     is instantiated
   * @return An SFLogger instance given the name
   */
  public static SFLogger getLogger(String name) {
    if (loggerImplementation == null) {
      String logger = systemGetProperty("net.snowflake.jdbc.loggerImpl");

      loggerImplementation = LoggerImpl.fromString(logger);

      if (loggerImplementation == null) {
        // default to use java util logging
        loggerImplementation = LoggerImpl.JDK14LOGGER;
      }
    }

    switch (loggerImplementation) {
      case SLF4JLOGGER:
        return new SLF4JLogger(name);
      case JDK14LOGGER:
      default:
        return new JDK14Logger(name);
    }
  }
}
