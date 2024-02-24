/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

public class ArrowResultSetStructuredTypesLatestIT extends ResultSetStructuredTypesLatestIT {
  public ArrowResultSetStructuredTypesLatestIT() {
    super(ResultSetFormatType.ARROW_WITH_JSON_STRUCTURED_TYPES);
  }
}
