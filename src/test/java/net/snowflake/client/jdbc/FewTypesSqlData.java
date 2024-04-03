package net.snowflake.client.jdbc;

import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

public class FewTypesSqlData implements SQLData {

  private String string;
  private Integer intValue;
  private Integer nullIntValue;
  private Long longValue;

  public FewTypesSqlData() {}

  @Override
  public String getSQLTypeName() throws SQLException {
    return null;
  }

  @Override
  public void readSQL(SQLInput stream, String typeName) throws SQLException {
    string = stream.readString();
    intValue = stream.readInt();
    nullIntValue = stream.readObject(Integer.class);
    longValue = stream.readLong();
  }

  @Override
  public void writeSQL(SQLOutput stream) throws SQLException {
    stream.writeString(string);
    stream.writeInt(intValue);
    stream.writeLong(longValue);
  }

  public String getString() {
    return string;
  }

  public Integer getIntValue() {
    return intValue;
  }

  public Integer getNullIntValue() {
    return nullIntValue;
  }

  public Long getLongValue() {
    return longValue;
  }
}
