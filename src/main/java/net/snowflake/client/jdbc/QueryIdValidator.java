package net.snowflake.client.jdbc;

import java.util.regex.Pattern;

class QueryIdValidator {
  private static final Pattern QUERY_ID_REGEX =
      Pattern.compile("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}");

  static boolean isValid(String queryId) {
    return queryId != null && QUERY_ID_REGEX.matcher(queryId).matches();
  }
}
