package net.snowflake.client.core.structs;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface SFSqlInput {
  String readString(String fieldName) throws SQLException;
  Boolean readBoolean(String fieldName) throws SQLException;
  <T extends SFSqlData> T readObject(String fieldName, Class<T> type) throws SQLException;
  <T extends SFSqlData> List<T> readList(String fieldName, Class<T> type) throws SQLException;
  <T extends SFSqlData> T[] readArray(String fieldName, Class<T> type) throws SQLException;
  <K, T extends SFSqlData> Map<K, T > readMap(String fieldName, Class<K> keyType, Class<T> type) throws SQLException;
}
