package net.snowflake.client.jdbc.telemetry;

public enum TelemetryField {
  // we use "client_" as a prefix for all metrics on the client side
  TIME_CONSUME_FIRST_RESULT("client_time_consume_first_result"),
  TIME_CONSUME_LAST_RESULT("client_time_consume_last_result"),
  TIME_WAITING_FOR_CHUNKS("client_time_waiting_for_chunks"),
  TIME_DOWNLOADING_CHUNKS("client_time_downloading_chunks"),
  TIME_PARSING_CHUNKS("client_time_parsing_chunks"),

  FAILED_BIND_SERIALIZATION("client_failed_bind_serialization"),
  FAILED_BIND_UPLOAD("client_failed_bind_upload"),
  FAILED_BIND_OTHER("client_failed_bind_other"),

  SQL_EXCEPTION("client_sql_exception"),

  METADATA_METRICS("client_metadata_api_metrics");

  public final String field;

  TelemetryField(String field) {
    this.field = field;
  }

  @Override
  public String toString() {
    return field;
  }
}
