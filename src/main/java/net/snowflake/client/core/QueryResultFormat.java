package net.snowflake.client.core;

import java.util.Optional;

public enum QueryResultFormat {
  JSON("json"),
  ARROW("arrow");

  private String name;

  QueryResultFormat(String name) {
    this.name = name;
  }

  public static Optional<QueryResultFormat> lookupByName(String n) {
    for (QueryResultFormat format : QueryResultFormat.values()) {
      if (format.name.equalsIgnoreCase(n)) {
        return Optional.of(format);
      }
    }

    return Optional.empty();
  }
}
