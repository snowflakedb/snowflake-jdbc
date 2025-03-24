package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import net.snowflake.client.core.EventHandler;
import net.snowflake.client.core.EventUtil;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.util.SecretDetector;

/**
 * Use java.util.logging to implements SFLogger.
 *
 * <p>Log Level mapping from SFLogger to java.util.logging: ERROR -- SEVERE WARN -- WARNING INFO --
 * INFO DEBUG -- FINE TRACE -- FINEST
 */
public class JDK14Logger implements SFLogger {
  private Logger jdkLogger;

  private Set<String> logMethods =
      new HashSet<>(Arrays.asList("debug", "error", "info", "trace", "warn", "debugNoMask"));

  private static boolean isLoggerInit = false;

  public static String STDOUT = "STDOUT";

  private static final StdOutConsoleHandler STD_OUT_CONSOLE_HANDLER = new StdOutConsoleHandler();

  public JDK14Logger(String name) {
    this.jdkLogger = Logger.getLogger(name);
  }

  static {
    String javaLoggingConsoleStdOut =
        System.getProperty(SFSessionProperty.JAVA_LOGGING_CONSOLE_STD_OUT.getPropertyKey());
    if ("true".equalsIgnoreCase(javaLoggingConsoleStdOut)) {
      String javaLoggingConsoleStdOutThreshold =
          System.getProperty(
              SFSessionProperty.JAVA_LOGGING_CONSOLE_STD_OUT_THRESHOLD.getPropertyKey());
      useStdOutConsoleHandler(javaLoggingConsoleStdOutThreshold);
    }
  }

  @SnowflakeJdbcInternalApi
  public static void useStdOutConsoleHandler(String threshold) {
    Level thresholdLevel = threshold != null ? tryParse(threshold) : null;
    Logger rootLogger = Logger.getLogger("");
    for (Handler handler : rootLogger.getHandlers()) {
      if (handler instanceof ConsoleHandler) {
        rootLogger.removeHandler(handler);
        if (thresholdLevel != null) {
          rootLogger.addHandler(new StdErrOutThresholdAwareConsoleHandler(thresholdLevel));
        } else {
          rootLogger.addHandler(new StdOutConsoleHandler());
        }
        break;
      }
    }
  }

  private static Level tryParse(String threshold) {
    try {
      return Level.parse(threshold);
    } catch (Exception e) {
      throw new UnknownJavaUtilLoggingLevelException(threshold);
    }
  }

  @SnowflakeJdbcInternalApi
  static void resetToDefaultConsoleHandler() {
    Logger rootLogger = Logger.getLogger("");
    for (Handler handler : rootLogger.getHandlers()) {
      if (handler instanceof StdErrOutThresholdAwareConsoleHandler
          || handler instanceof StdOutConsoleHandler) {
        rootLogger.removeHandler(handler);
        rootLogger.addHandler(new ConsoleHandler());
        break;
      }
    }
  }

  public boolean isDebugEnabled() {
    return this.jdkLogger.isLoggable(Level.FINE);
  }

  public boolean isErrorEnabled() {
    return this.jdkLogger.isLoggable(Level.SEVERE);
  }

  public boolean isInfoEnabled() {
    return this.jdkLogger.isLoggable(Level.INFO);
  }

  public boolean isTraceEnabled() {
    return this.jdkLogger.isLoggable(Level.FINEST);
  }

  public boolean isWarnEnabled() {
    return this.jdkLogger.isLoggable(Level.WARNING);
  }

  public void debug(String msg, boolean isMasked) {
    logInternal(Level.FINE, msg, isMasked);
  }

  // This function is used to display unmasked, potentially sensitive log information for internal
  // regression testing purposes. Do not use otherwise.
  public void debugNoMask(String msg) {
    logInternal(Level.FINE, msg, false);
  }

  public void debug(String msg, Object... arguments) {
    logInternal(Level.FINE, msg, arguments);
  }

  public void debug(String msg, Throwable t) {
    logInternal(Level.FINE, msg, t);
  }

  public void error(String msg, boolean isMasked) {
    logInternal(Level.SEVERE, msg, isMasked);
  }

  public void error(String msg, Object... arguments) {
    logInternal(Level.SEVERE, msg, arguments);
  }

  public void error(String msg, Throwable t) {
    logInternal(Level.SEVERE, msg, t);
  }

  public void info(String msg, boolean isMasked) {
    logInternal(Level.INFO, msg, isMasked);
  }

  public void info(String msg, Object... arguments) {
    logInternal(Level.INFO, msg, arguments);
  }

  public void info(String msg, Throwable t) {
    logInternal(Level.INFO, msg, t);
  }

  public void trace(String msg, boolean isMasked) {
    logInternal(Level.FINEST, msg, isMasked);
  }

  public void trace(String msg, Object... arguments) {
    logInternal(Level.FINEST, msg, arguments);
  }

  public void trace(String msg, Throwable t) {
    logInternal(Level.FINEST, msg, t);
  }

  public void warn(String msg, boolean isMasked) {
    logInternal(Level.WARNING, msg, isMasked);
  }

  public void warn(String msg, Object... arguments) {
    logInternal(Level.WARNING, msg, arguments);
  }

  public void warn(String msg, Throwable t) {
    logInternal(Level.WARNING, msg, t);
  }

  private void logInternal(Level level, String msg, boolean masked) {
    if (jdkLogger.isLoggable(level)) {
      String[] source = findSourceInStack();
      jdkLogger.logp(
          level, source[0], source[1], masked == true ? SecretDetector.maskSecrets(msg) : msg);
    }
  }

  private void logInternal(Level level, String msg, Object... arguments) {
    if (jdkLogger.isLoggable(level)) {
      String[] source = findSourceInStack();
      String message = "";
      try {
        message = MessageFormat.format(refactorString(msg), evaluateLambdaArgs(arguments));
      } catch (IllegalArgumentException e) {
        message = "Unable to format msg: " + msg;
      }
      jdkLogger.logp(level, source[0], source[1], SecretDetector.maskSecrets(message));
    }
  }

  private void logInternal(Level level, String msg, Throwable t) {
    // add logger message here
    if (jdkLogger.isLoggable(level)) {
      String[] source = findSourceInStack();
      jdkLogger.logp(level, source[0], source[1], SecretDetector.maskSecrets(msg), t);
    }
  }

  public static void addHandler(Handler handler) {
    Logger snowflakeLogger = Logger.getLogger(SFFormatter.CLASS_NAME_PREFIX);
    snowflakeLogger.addHandler(handler);
  }

  public static void removeHandler(Handler handler) {
    Logger snowflakeLogger = Logger.getLogger(SFFormatter.CLASS_NAME_PREFIX);
    snowflakeLogger.removeHandler(handler);
  }

  public static void setUseParentHandlers(boolean value) {
    Logger snowflakeLogger = Logger.getLogger(SFFormatter.CLASS_NAME_PREFIX);
    snowflakeLogger.setUseParentHandlers(value);
  }

  public static void setLevel(Level level) {
    Logger snowflakeLogger = Logger.getLogger(SFFormatter.CLASS_NAME_PREFIX);
    snowflakeLogger.setLevel(level);
  }

  public static Level getLevel() {
    Logger snowflakeLogger = Logger.getLogger(SFFormatter.CLASS_NAME_PREFIX);
    return snowflakeLogger.getLevel();
  }

  /**
   * This is way to enable logging in JDBC through TRACING parameter or sf client config file.
   *
   * @param level log level
   * @param logPath log path
   * @throws IOException if there is an error writing to the log
   */
  public static synchronized void instantiateLogger(Level level, String logPath)
      throws IOException {
    if (!isLoggerInit) {
      loggerInit(level, logPath);
      isLoggerInit = true;
    }
  }

  /**
   * Since we use SLF4J ways of formatting string we need to refactor message string if we have
   * arguments. For example, in sl4j, this string can be formatted with 2 arguments
   *
   * <p>ex.1: Error happened in {} on {}
   *
   * <p>And if two arguments are provided, error message can be formatted.
   *
   * <p>However, in java.util.logging, to achieve formatted error message, Same string should be
   * converted to
   *
   * <p>ex.2: Error happened in {0} on {1}
   *
   * <p>Which represented first arguments and second arguments will be replaced in the corresponding
   * places.
   *
   * <p>This method will convert string in ex.1 to ex.2
   *
   * @param original original string
   * @return refactored string
   */
  private String refactorString(String original) {
    StringBuilder sb = new StringBuilder();
    int argCount = 0;
    for (int i = 0; i < original.length(); i++) {
      if (original.charAt(i) == '{' && i < original.length() - 1 && original.charAt(i + 1) == '}') {
        sb.append(String.format("{%d}", argCount));
        argCount++;
        i++;
      } else {
        sb.append(original.charAt(i));
      }
    }
    return sb.toString();
  }

  /**
   * Used to find the index of the source class/method in current stack This method will locate the
   * source as the first method after logMethods
   *
   * @return an array of size two, first element is className and second is methodName
   */
  private String[] findSourceInStack() {
    StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
    String[] results = new String[2];
    for (int i = 0; i < stackTraces.length; i++) {
      if (logMethods.contains(stackTraces[i].getMethodName())) {
        // since already find the highest logMethods, find the first method after this one
        // and is not a logMethods. This is done to avoid multiple wrapper over log methods
        for (int j = i; j < stackTraces.length; j++) {
          if (!logMethods.contains(stackTraces[j].getMethodName())) {
            results[0] = stackTraces[j].getClassName();
            results[1] = stackTraces[j].getMethodName();
            return results;
          }
        }
      }
    }
    return results;
  }

  private static void loggerInit(Level level, String outputPath) throws IOException {
    Logger snowflakeLoggerInformaticaV1 =
        Logger.getLogger(SFFormatter.INFORMATICA_V1_CLASS_NAME_PREFIX);

    // setup event handler
    EventHandler eventHandler = EventUtil.getEventHandlerInstance();
    eventHandler.setLevel(Level.INFO);
    eventHandler.setFormatter(new SimpleFormatter());
    JDK14Logger.addHandler(eventHandler);

    snowflakeLoggerInformaticaV1.setLevel(level);
    JDK14Logger.setLevel(level);
    snowflakeLoggerInformaticaV1.addHandler(eventHandler);

    if (STDOUT.equalsIgnoreCase(outputPath)) {
      // Initialize console handler
      ConsoleHandler consoleHandler = new ConsoleHandler();
      consoleHandler.setLevel(level);
      consoleHandler.setFormatter(new SFFormatter());
      JDK14Logger.addHandler(consoleHandler);
      snowflakeLoggerInformaticaV1.addHandler(consoleHandler);

    } else {
      // Initialize file handler.
      // get log count and size
      String defaultLogSizeVal = systemGetProperty("snowflake.jdbc.log.size");
      String defaultLogCountVal = systemGetProperty("snowflake.jdbc.log.count");

      // default log size to 1 GB
      int logSize = 1000000000;

      // default number of log files to rotate to 2
      int logCount = 2;

      if (defaultLogSizeVal != null) {
        try {
          logSize = Integer.parseInt(defaultLogSizeVal);
        } catch (Exception ex) {
          ;
        }
      }

      if (defaultLogCountVal != null) {
        try {
          logCount = Integer.parseInt(defaultLogCountVal);
        } catch (Exception ex) {
          ;
        }
      }

      // write log file to tmp directory
      FileHandler fileHandler = new FileHandler(outputPath, logSize, logCount, true);
      fileHandler.setFormatter(new SFFormatter());
      fileHandler.setLevel(level);

      JDK14Logger.addHandler(fileHandler);
      snowflakeLoggerInformaticaV1.addHandler(fileHandler);
    }
  }

  private static Object[] evaluateLambdaArgs(Object... args) {
    final Object[] result = new Object[args.length];

    for (int i = 0; i < args.length; i++) {
      result[i] = args[i] instanceof ArgSupplier ? ((ArgSupplier) args[i]).get() : args[i];
    }

    return result;
  }
}
