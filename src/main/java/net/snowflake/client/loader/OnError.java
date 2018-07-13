/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.loader;

import java.util.regex.Pattern;

/**
 * COPY ON_ERROR option
 */
class OnError
{
  private final static Pattern validPattern = Pattern.compile(
      "(?i)(?:ABORT_STATEMENT|CONTINUE|SKIP_FILE(?:_\\d+%?)?)"
  );

  /**
   * Default behavior for ON_ERROR for Loader API.
   */
  final static String DEFAULT = "CONTINUE";

  private OnError()
  {
  }

  /**
   * Validates ON_ERROR value and return true if valid otherwise false.
   *
   * @param value ON_ERROR value
   * @return true if valid otherwise false.
   */
  static boolean validate(String value)
  {
    return value != null && validPattern.matcher(value).matches();
  }
}
