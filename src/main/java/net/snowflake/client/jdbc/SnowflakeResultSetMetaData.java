package net.snowflake.client.jdbc;

import java.sql.SQLException;
import java.util.List;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

public interface SnowflakeResultSetMetaData {
  String getQueryID() throws SQLException;

  List<String> getColumnNames() throws SQLException;

  int getColumnIndex(String columnName) throws SQLException;

  int getInternalColumnType(int column) throws SQLException;

  @SnowflakeJdbcInternalApi
  List<FieldMetadata> getColumnFields(int column) throws SQLException;
}
