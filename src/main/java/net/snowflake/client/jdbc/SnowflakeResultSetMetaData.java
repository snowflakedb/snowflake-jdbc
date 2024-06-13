package net.snowflake.client.jdbc;

import java.sql.SQLException;
import java.util.List;

public interface SnowflakeResultSetMetaData {
  String getQueryID() throws SQLException;

  List<String> getColumnNames() throws SQLException;

  int getColumnIndex(String columnName) throws SQLException;

  int getInternalColumnType(int column) throws SQLException;

  List<FieldMetadata> getColumnFields(int column) throws SQLException;

  /**
   * Get vector dimension
   *
   * @param column column index
   * @return vector dimension when the column is vector type or 0 when it is not vector type
   * @throws SQLException when cannot get column dimension
   */
  int getDimension(int column) throws SQLException;

  /**
   * Get vector dimension
   *
   * @param columnName column name
   * @return vector dimension when the column is vector type or 0 when it is not vector type
   * @throws SQLException when cannot get column dimension
   */
  int getDimension(String columnName) throws SQLException;
}
