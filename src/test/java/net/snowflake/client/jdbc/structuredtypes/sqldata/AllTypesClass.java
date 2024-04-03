package net.snowflake.client.jdbc.structuredtypes.sqldata;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.Time;
import java.sql.Timestamp;

public class AllTypesClass implements SQLData {
  private String string;
  private Byte b;
  private Short s;
  private Integer i;
  private Long l;
  private Float f;
  private Double d;
  private BigDecimal bd;
  private Boolean bool;
  private Timestamp timestampLtz;
  private Timestamp timestampNtz;
  private Timestamp timestampTz;
  private Date date;
  private Time time;
  private byte[] binary;
  private SimpleClass simpleClass;

  @Override
  public String getSQLTypeName() throws SQLException {
    return null;
  }

  @Override
  public void readSQL(SQLInput sqlInput, String typeName) throws SQLException {
    string = sqlInput.readString();
    if (sqlInput.wasNull()) string = null;
    b = sqlInput.readByte();
    if (sqlInput.wasNull()) b = null;
    s = sqlInput.readShort();
    if (sqlInput.wasNull()) s = null;
    i = sqlInput.readInt();
    if (sqlInput.wasNull()) i = null;
    l = sqlInput.readLong();
    if (sqlInput.wasNull()) l = null;
    f = sqlInput.readFloat();
    if (sqlInput.wasNull()) f = null;
    d = sqlInput.readDouble();
    if (sqlInput.wasNull()) d = null;
    bd = sqlInput.readBigDecimal();
    bool = sqlInput.readBoolean();
    if (sqlInput.wasNull()) bool = null;
    timestampLtz = sqlInput.readTimestamp();
    timestampNtz = sqlInput.readTimestamp();
    timestampTz = sqlInput.readTimestamp();
    date = sqlInput.readDate();
    time = sqlInput.readTime();
    binary = sqlInput.readBytes();
    simpleClass = sqlInput.readObject(SimpleClass.class);
  }

  @Override
  public void writeSQL(SQLOutput stream) throws SQLException {}

  public String getString() {
    return string;
  }

  public Byte getB() {
    return b;
  }

  public Short getS() {
    return s;
  }

  public Integer getI() {
    return i;
  }

  public Long getL() {
    return l;
  }

  public Float getF() {
    return f;
  }

  public Double getD() {
    return d;
  }

  public BigDecimal getBd() {
    return bd;
  }

  public Boolean getBool() {
    return bool;
  }

  public Timestamp getTimestampLtz() {
    return timestampLtz;
  }

  public Timestamp getTimestampNtz() {
    return timestampNtz;
  }

  public Timestamp getTimestampTz() {
    return timestampTz;
  }

  public Date getDate() {
    return date;
  }

  public Time getTime() {
    return time;
  }

  public byte[] getBinary() {
    return binary;
  }

  public SimpleClass getSimpleClass() {
    return simpleClass;
  }
}
