package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.util.stream.Collectors;
import java.util.stream.Stream;

enum CommonsLoggingWrapperMode {
  /** All logs from commons logging are passed to SFLogger (check {@link CommonsLoggingWrapper}) */
  ALL,
  /**
   * The default behaviour is forwarding all logs to java.util.logging from commons logging (check
   * {@link JDK14JCLWrapper}), no logs are forwarded to SLF4J logger (check {@link SLF4JJCLWrapper})
   */
  DEFAULT,
  /**
   * Logs from commons logging are not forwarded and commons logging is not reconfigured - may be
   * the option when if you need to replace commons logging with the SLF4J bridge when thin jar is
   * used
   */
  OFF;

  static final String JAVA_PROPERTY = "net.snowflake.jdbc.commons_logging_wrapper";

  static CommonsLoggingWrapperMode detect() {
    String value = systemGetProperty(JAVA_PROPERTY);
    if (value == null) {
      return DEFAULT;
    }
    try {
      return CommonsLoggingWrapperMode.valueOf(value);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Unknown commons logging wrapper value '"
              + value
              + "', expected one of: "
              + Stream.of(CommonsLoggingWrapperMode.values())
                  .map(Enum::name)
                  .collect(Collectors.joining(", ")));
    }
  }
}
