/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.SQLException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;

/**
 * Factory class to create SFBaseResultSet class. Depending on result format, different instance
 * will be created
 */
class SFResultSetFactory {
  /**
   * Factory class used to generate ResultSet object according to query result format
   *
   * @param result raw response from server
   * @param statement statement that created current resultset
   * @param sortResult true if sort first chunk
   * @return result set object
   */
  static SFBaseResultSet getResultSet(JsonNode result, SFStatement statement, boolean sortResult)
      throws SQLException {

    // This should only be invoked from an SFSession connection
    SFSession session = (SFSession) statement.getSFBaseSession();
    SnowflakeResultSetSerializableV1 resultSetSerializable =
        SnowflakeResultSetSerializableV1.create(result, session, statement);

    switch (resultSetSerializable.getQueryResultFormat()) {
      case ARROW:
        return new SFArrowResultSet(resultSetSerializable, session, statement, sortResult);
      case JSON:
        return new SFResultSet(resultSetSerializable, statement, sortResult);
      default:
        throw new SnowflakeSQLLoggedException(
            statement.getSFBaseSession(),
            ErrorCode.INTERNAL_ERROR,
            "Unsupported query result format: "
                + resultSetSerializable.getQueryResultFormat().name());
    }
  }
}
