package net.snowflake.client.log;

import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class UnknownJavaUtilLoggingLevelException extends RuntimeException {
  private static final String AVAILABLE_LEVELS =
      Stream.of(
              Level.OFF,
              Level.SEVERE,
              Level.WARNING,
              Level.INFO,
              Level.CONFIG,
              Level.FINE,
              Level.FINER,
              Level.FINEST,
              Level.ALL)
          .map(Level::getName)
          .collect(Collectors.joining(", "));

  UnknownJavaUtilLoggingLevelException(String threshold) {
    super(
        "Unknown java util logging level: " + threshold + ", expected one of: " + AVAILABLE_LEVELS);
  }
}
