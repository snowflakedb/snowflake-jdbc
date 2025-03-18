package net.snowflake.client.jdbc;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class ColumnTypeInfo {
  private int columnType;
  private String extColTypeName;
  private SnowflakeType snowflakeType;

  public ColumnTypeInfo(int columnType, String extColTypeName, SnowflakeType snowflakeType) {
    this.columnType = columnType;
    this.extColTypeName = extColTypeName;
    this.snowflakeType = snowflakeType;
  }

  public int getColumnType() {
    return columnType;
  }

  public String getExtColTypeName() {
    return extColTypeName;
  }

  public SnowflakeType getSnowflakeType() {
    return snowflakeType;
  }
}
