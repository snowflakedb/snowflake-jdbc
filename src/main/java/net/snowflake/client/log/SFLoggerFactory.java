package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Used to create SFLogger instance */
public class SFLoggerFactory {
  private static LoggerImpl loggerImplementation;

  private static final List<ReconfigurableSFLogger> registeredLoggers =
      Collections.synchronizedList(new ArrayList<>());

  private static Mode MODE = Mode.BUFFERING;

  public static void reconfigure() {
    MODE = Mode.INITIALIZED;
    for (ReconfigurableSFLogger logger : registeredLoggers) {
      logger.reconfigure(createLogger(logger.getName()));
    }
    registeredLoggers.clear();
  }

  public enum Mode {
    BUFFERING,
    INITIALIZED
  }

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
    return getLogger(clazz.getName());
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
    if (MODE == Mode.INITIALIZED) {
      return createLogger(name);
    }
    ReconfigurableSFLogger logger = new ReconfigurableSFLogger(name);
    registeredLoggers.add(logger);
    return logger;
  }

  private static SFLogger createLogger(String name) {
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
