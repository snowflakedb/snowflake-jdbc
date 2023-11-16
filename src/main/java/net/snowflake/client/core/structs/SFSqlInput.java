package net.snowflake.client.core.structs;

import java.sql.SQLException;

public interface SFSqlInput {
  String readString(String fieldName) throws SQLException;
}
