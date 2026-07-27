package net.snowflake.client.internal.jdbc.telemetry;

// TODO: SNOW-2223750 Refactor this enum, as it contains the possible values of the field "type" and
//  is misleading. Separate values from the field names.
public enum TelemetryField {
  // Fields
  TYPE("type"),
  VALUE("value"),
  // TODO(SNOW-3548350): SOURCE is used by client_connection_identifier_shape (mirrors the
  //  cross-driver "source" key in the Go/Python/Node.js drivers). May become more broadly
  //  applicable; for now it lives here because the shape telemetry is the only consumer.
  SOURCE("source"),
  DRIVER_TYPE("DriverType"),
  DRIVER_VERSION("DriverVersion"),
  QUERY_ID("QueryID"),
  SQL_STATE("SQLState"),
  ERROR_NUMBER("ErrorNumber"),
  ERROR_MESSAGE("ErrorMessage"),
  REASON("reason"),

  // Values of the field "type"
  // we use "client_" as a prefix for all metrics on the client side
  TIME_CONSUME_FIRST_RESULT("client_time_consume_first_result"),
  TIME_CONSUME_LAST_RESULT("client_time_consume_last_result"),
  TIME_WAITING_FOR_CHUNKS("client_time_waiting_for_chunks"),
  TIME_DOWNLOADING_CHUNKS("client_time_downloading_chunks"),
  TIME_PARSING_CHUNKS("client_time_parsing_chunks"),
  TIME_DOWNLOADING_CRL("client_time_downloading_crl"),
  TIME_PARSING_CRL("client_time_parsing_crl"),

  CLIENT_CRL_STATS("client_crl_stats"),
  CRL_URL("client_crl_url"),
  CRL_BYTES("client_crl_bytes"),
  CRL_REVOKED_CERTIFICATES("client_revoked_certificates"),

  FAILED_BIND_SERIALIZATION("client_failed_bind_serialization"),
  FAILED_BIND_UPLOAD("client_failed_bind_upload"),
  FAILED_BIND_OTHER("client_failed_bind_other"),

  SQL_EXCEPTION("client_sql_exception"),

  // TODO(SNOW-3548350): emitted at most once per successful login; describes which
  //  connection-identifier fields the user supplied (no hostname or account value is ever
  //  included). See ConnectionIdentifierShape + ConnectionIdentifierShapeTelemetry.
  CONNECTION_IDENTIFIER_SHAPE("client_connection_identifier_shape"),

  METADATA_METRICS("client_metadata_api_metrics"),

  HTTP_EXCEPTION("client_http_exception"),
  OCSP_EXCEPTION("client_ocsp_exception"),

  INTERNAL_API_USAGE("client_internal_api_usage"),

  HEARTBEAT_MAX_THREADS_EXCEEDED("client_heartbeat_max_threads_exceeded");

  public final String field;

  TelemetryField(String field) {
    this.field = field;
  }

  @Override
  public String toString() {
    return field;
  }
}
