/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.util.stream.Collectors;
import java.util.stream.Stream;

enum CommonsLoggingWrapperMode {
  ALL,
  DEFAULT,
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
