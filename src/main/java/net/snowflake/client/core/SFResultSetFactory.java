/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;

import java.sql.SQLException;

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
  static SFBaseResultSet getResultSet(JsonNode result, SFStatement statement, boolean sortResult, String sqlText)
          throws SQLException {

    SnowflakeResultSetSerializableV1 resultSetSerializable =
        SnowflakeResultSetSerializableV1.create(result, statement.getSFBaseSession(), statement);

    JsonNode resultIds = result.path("data").path("resultIds");
    boolean multistatement = true;
    if (resultIds.isNull() || resultIds.isMissingNode() || resultIds.asText().isEmpty()) {
      multistatement = false;
    }
    // Store select statements in cache
    SFBaseSession session = statement.getSFBaseSession();
    if (session.useResultCache && resultSetSerializable.getStatementType().isSelect() && !multistatement)
    {
      session.resultCache.putResult(sqlText, resultSetSerializable.getQueryId(),  session.getSessionId(), resultSetSerializable);
    }

    switch (resultSetSerializable.getQueryResultFormat()) {
      case ARROW:
        return new SFArrowResultSet(
            resultSetSerializable, statement.getSFBaseSession(), statement, sortResult);
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

  static SFBaseResultSet getResultFromCache(
      Object resultSetSerializable, SFStatement statement, boolean sortResult) throws SQLException {
    if (resultSetSerializable instanceof SnowflakeResultSetSerializableV1) {
      QueryResultFormat format =
          ((SnowflakeResultSetSerializableV1) resultSetSerializable).getQueryResultFormat();
      switch (format) {
        case ARROW:
          return new SFArrowResultSet(
              (SnowflakeResultSetSerializableV1) resultSetSerializable,
              statement.getSFBaseSession(),
              statement,
              sortResult);
        case JSON:
          return new SFResultSet(
              (SnowflakeResultSetSerializableV1) resultSetSerializable, statement, sortResult);
        default:
          throw new SnowflakeSQLLoggedException(
              statement.getSFBaseSession(),
              ErrorCode.INTERNAL_ERROR,
              "Unsupported query result format: " + format.name());
      }
    }
    throw new SnowflakeSQLLoggedException(
        statement.getSFBaseSession(),
        ErrorCode.INTERNAL_ERROR,
        "Unsupported result format stored in cache. Must be ResultSetSerializableV1");
  }
}
