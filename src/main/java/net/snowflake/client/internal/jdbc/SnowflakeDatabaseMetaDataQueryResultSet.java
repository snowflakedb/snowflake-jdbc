package net.snowflake.client.internal.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Database Metadata query based result set. */
public class SnowflakeDatabaseMetaDataQueryResultSet extends SnowflakeDatabaseMetaDataResultSet {

  public SnowflakeDatabaseMetaDataQueryResultSet(
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
