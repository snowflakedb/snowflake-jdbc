package net.snowflake.client.jdbc.structuredtypes.sqldata;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

public class NullableFieldsSqlData implements SQLData {

  private String string;
  private Integer nullableIntValue;
  private Long nullableLongValue;
  private Date date;
  private BigDecimal bd;
  private byte[] bytes;
  private long longValue;

  public NullableFieldsSqlData() {}

  @Override
  public String getSQLTypeName() throws SQLException {
    return null;
  }

  @Override
  public void readSQL(SQLInput stream, String typeName) throws SQLException {
    string = stream.readString();
    nullableIntValue = stream.readObject(Integer.class);
    nullableLongValue = stream.readObject(Long.class);
    date = stream.readObject(Date.class);
    bd = stream.readBigDecimal();
    bytes = stream.readObject(byte[].class);
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

  public Date getDate() {
    return date;
  }

  public BigDecimal getBd() {
    return bd;
  }

  public byte[] getBytes() {
    return bytes;
  }
}
