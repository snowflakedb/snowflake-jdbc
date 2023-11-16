package net.snowflake.client.core.structs;

import java.sql.SQLException;

public interface SFSqlData {
  void readSql(SFSqlInput sqlInput) throws SQLException;
  void writeSql(SFSqlOutput sqlOutput) throws SQLException;
}
