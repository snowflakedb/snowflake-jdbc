package net.snowflake.client.core.structs;

import java.sql.SQLException;

public interface SFSqlInput {
  String readString(String fieldName) throws SQLException;
  Boolean readBoolean(String fieldName) throws SQLException;
  <T extends SFSqlData> T readObject(String fieldName, Class<T> type) throws SQLException;
}
