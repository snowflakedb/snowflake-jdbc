/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;

import java.sql.SQLException;

/**
 * Factory class to create SFBaseResultSet class. Depending on result
 * format, different instance will be created
 */
class SFResultSetFactory
{
  /**
   * Factory class used to generate ResultSet object according to query result
   * format
   *
   * @param result     raw response from server
   * @param statement  statement that created current resultset
   * @param sortResult true if sort first chunk
   * @return result set object
   */
  static SFBaseResultSet getResultSet(JsonNode result,
                                      SFStatement statement,
                                      boolean sortResult)
  throws SQLException
  {
    SFSession session = statement.getSession();

    ResultUtil.ResultInput resultInput = new ResultUtil.ResultInput();
    resultInput.setResultJSON(result)
        .setConnectionTimeout(session.getHttpClientConnectionTimeout())
        .setSocketTimeout(session.getHttpClientSocketTimeout())
        .setNetworkTimeoutInMilli(session.getNetworkTimeoutInMilli());

    ResultUtil.ResultOutput resultOutput = ResultUtil
        .processResult(resultInput, statement);

    switch (resultOutput.queryResultFormat)
    {
      case ARROW:
        return new SFArrowResultSet(resultOutput, statement, sortResult);
      case JSON:
        return new SFResultSet(resultOutput, statement, sortResult);
      default:
        throw new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR,
                                        "Unsupported query result format: " +
                                        resultOutput.queryResultFormat.name());
    }
  }
}
