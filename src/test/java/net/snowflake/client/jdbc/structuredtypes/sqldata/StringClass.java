package net.snowflake.client.jdbc.structuredtypes.sqldata;

import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

public class StringClass implements SQLData {
  public String getString() {
    return string;
  }

  private String string;

  public StringClass() {}

  @Override
  public String getSQLTypeName() throws SQLException {
    return null;
  }

  @Override
  public void readSQL(SQLInput stream, String typeName) throws SQLException {
    string = stream.readString();
  }

  @Override
  public void writeSQL(SQLOutput stream) throws SQLException {
    stream.writeString(string);
  }
}
