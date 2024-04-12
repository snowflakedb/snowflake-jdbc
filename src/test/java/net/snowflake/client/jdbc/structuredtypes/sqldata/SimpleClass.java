package net.snowflake.client.jdbc.structuredtypes.sqldata;

import java.beans.Transient;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import net.snowflake.client.jdbc.SnowflakeColumn;

public class SimpleClass implements SQLData {

  @SnowflakeColumn(length = 12)
  private String string;

  @SnowflakeColumn(precision = 36, scale = 4)
  private Integer intValue;

  public SimpleClass() {}

  public SimpleClass(String x, Integer y) {
    this.string = x;
    this.intValue = y;
  }

  @Override
  @Transient
  public String getSQLTypeName() throws SQLException {
    return null;
  }

  @Override
  public void readSQL(SQLInput stream, String typeName) throws SQLException {
    string = stream.readString();
    intValue = stream.readInt();
  }

  @Override
  public void writeSQL(SQLOutput stream) throws SQLException {
    stream.writeString(string);
    stream.writeInt(intValue);
  }

  public String getString() {
    return string;
  }

  public Integer getIntValue() {
    return intValue;
  }
}
