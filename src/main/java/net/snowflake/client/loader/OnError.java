package net.snowflake.client.loader;

import java.util.regex.Pattern;

/** COPY ON_ERROR option */
class OnError {
  private static final Pattern validPattern =
      Pattern.compile("(?i)(?:ABORT_STATEMENT|CONTINUE|SKIP_FILE(?:_\\d+%?)?)");

  /** Default behavior for ON_ERROR for Loader API. */
  static final String DEFAULT = "CONTINUE";

  private OnError() {}

  /**
   * Validates ON_ERROR value and return true if valid otherwise false.
   *
   * @param value ON_ERROR value
   * @return true if valid otherwise false.
   */
  static boolean validate(String value) {
    return value != null && validPattern.matcher(value).matches();
  }
}
