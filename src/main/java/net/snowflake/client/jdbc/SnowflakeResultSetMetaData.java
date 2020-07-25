package net.snowflake.client.jdbc;

import java.sql.SQLException;
import java.util.List;

public interface SnowflakeResultSetMetaData {
  String getQueryID() throws SQLException;

  List<String> getColumnNames() throws SQLException;

  int getColumnIndex(String columnName) throws SQLException;

  int getInternalColumnType(int column) throws SQLException;
}
