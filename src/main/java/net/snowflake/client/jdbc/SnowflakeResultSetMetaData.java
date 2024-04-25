package net.snowflake.client.jdbc;

import java.sql.SQLException;
import java.util.List;

public interface SnowflakeResultSetMetaData {
  String getQueryID() throws SQLException;

  // Just a test
  List<String> getColumnNames() throws SQLException;

  int getColumnIndex(String columnName) throws SQLException;

  int getInternalColumnType(int column) throws SQLException;

  List<FieldMetadata> getColumnFields(int column) throws SQLException;
}
