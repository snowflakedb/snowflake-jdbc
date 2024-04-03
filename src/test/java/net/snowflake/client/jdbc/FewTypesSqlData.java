package net.snowflake.client.jdbc;

import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

public class FewTypesSqlData implements SQLData {

  private String string;
  private Integer nullableIntValue;
  private Long nullableLongValue;
  private Long longValue;

  public FewTypesSqlData() {}

  @Override
  public String getSQLTypeName() throws SQLException {
    return null;
  }

  @Override
  public void readSQL(SQLInput stream, String typeName) throws SQLException {
    string = stream.readString();
    nullableIntValue = stream.readInt();
    if(stream.wasNull()) {
      nullableIntValue = null;
    }
    nullableLongValue = stream.readObject(Long.class);
    longValue = stream.readLong();
  }

  @Override
  public void writeSQL(SQLOutput stream) throws SQLException {
    stream.writeString(string);
    stream.writeInt(nullableIntValue);
    stream.writeLong(longValue);
  }

  public String getString() {
    return string;
  }

  public Integer getNullableIntValue() {
    return nullableIntValue;
  }

  public Long getNullableLongValue() {
    return nullableLongValue;
  }

  public Long getLongValue() {
    return longValue;
  }
}
