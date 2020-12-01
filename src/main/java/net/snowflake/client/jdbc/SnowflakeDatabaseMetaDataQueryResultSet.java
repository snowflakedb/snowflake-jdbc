/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Database Metadata query based result set. */
class SnowflakeDatabaseMetaDataQueryResultSet extends SnowflakeDatabaseMetaDataResultSet {

  SnowflakeDatabaseMetaDataQueryResultSet(
      DBMetadataResultSetMetadata metadataType, ResultSet resultSet, Statement statement)
      throws SQLException {
    super(
        metadataType.getColumnNames(),
        metadataType.getColumnTypeNames(),
        metadataType.getColumnTypes(),
        resultSet,
        statement);
  }

  /**
   * Query result set cannot tell the last row.
   *
   * @return n/a
   * @throws SQLException if the result set is closed or SQLFeatureNotSupportedException
   */
  @Override
  public boolean isLast() throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  /**
   * Query result set cannot tell after the last row.
   *
   * @return n/a
   * @throws SQLException if the result set is closed or SQLFeatureNotSupportedException
   */
  @Override
  public boolean isAfterLast() throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }
}
