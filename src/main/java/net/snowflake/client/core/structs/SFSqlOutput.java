package net.snowflake.client.core.structs;

public interface SFSqlOutput {
  void writeString(String fieldName, String value);
  void writeObject(String fieldName, Object value);
}
