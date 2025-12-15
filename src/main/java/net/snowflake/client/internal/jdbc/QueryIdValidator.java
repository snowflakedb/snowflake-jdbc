package net.snowflake.client.internal.jdbc;

import java.util.regex.Pattern;

public class QueryIdValidator {
  private static final Pattern QUERY_ID_REGEX =
      Pattern.compile("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}");

  public static boolean isValid(String queryId) {
    return queryId != null && QUERY_ID_REGEX.matcher(queryId).matches();
  }
}
