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
  static SFBaseResultSet getResultSet(
      JsonNode result,
      SFStatement statement,
      boolean sortResult,
      ExecTimeTelemetryData execTimeData)
      throws SQLException {

    execTimeData.setProcessResultChunkStart();
    SnowflakeResultSetSerializableV1 resultSetSerializable =
        SnowflakeResultSetSerializableV1.create(result, statement.getSFBaseSession(), statement);
    execTimeData.setProcessResultChunkEnd();
    SFBaseResultSet rs;
    execTimeData.setCreateResultSetStart();
    switch (resultSetSerializable.getQueryResultFormat()) {
      case ARROW:
        rs =
            new SFArrowResultSet(
                resultSetSerializable, statement.getSFBaseSession(), statement, sortResult);
        break;
      case JSON:
        rs = new SFResultSet(resultSetSerializable, statement, sortResult);
        break;
      default:
        rs = null;
        break;
    }
    execTimeData.setCreateResultSetEnd();
    if (rs == null) {
      throw new SnowflakeSQLLoggedException(
          statement.getSFBaseSession(),
          ErrorCode.INTERNAL_ERROR,
          "Unsupported query result format: "
              + resultSetSerializable.getQueryResultFormat().name());
    }
    return rs;
  }
}
