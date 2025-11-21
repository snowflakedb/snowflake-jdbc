package net.snowflake.client.internal.jdbc;

public enum ResultSetFormatType {
  JSON("JSON"),
  ARROW_WITH_JSON_STRUCTURED_TYPES("ARROW"),
  NATIVE_ARROW("ARROW");
  public final String sessionParameterTypeValue;

  ResultSetFormatType(String sessionParameterTypeValue) {
    this.sessionParameterTypeValue = sessionParameterTypeValue;
  }
}
