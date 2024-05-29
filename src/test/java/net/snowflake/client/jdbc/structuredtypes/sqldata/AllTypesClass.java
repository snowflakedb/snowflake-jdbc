package net.snowflake.client.jdbc.structuredtypes.sqldata;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.Time;
import java.sql.Timestamp;
import net.snowflake.client.jdbc.SnowflakeColumn;

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

  @SnowflakeColumn(type = "timestamp_ltz")
  private Timestamp timestampLtz;

  @SnowflakeColumn(type = "timestamp_ntz")
  private Timestamp timestampNtz;

  @SnowflakeColumn(type = "timestamp_tz")
  private Timestamp timestampTz;

  private Date date;
  private Time time;
  private byte[] binary;
  private SimpleClass simpleClass;

  public AllTypesClass() {}

  public AllTypesClass(
      String string,
      Byte b,
      Short s,
      Integer i,
      Long l,
      Float f,
      Double d,
      BigDecimal bd,
      Boolean bool,
      Timestamp timestampLtz,
      Timestamp timestampNtz,
      Timestamp timestampTz,
      Date date,
      Time time,
      byte[] binary,
      SimpleClass simpleClass) {
    this.string = string;
    this.b = b;
    this.s = s;
    this.i = i;
    this.l = l;
    this.f = f;
    this.d = d;
    this.bd = bd;
    this.bool = bool;
    this.timestampLtz = timestampLtz;
    this.timestampNtz = timestampNtz;
    this.timestampTz = timestampTz;
    this.date = date;
    this.time = time;
    this.binary = binary;
    this.simpleClass = simpleClass;
  }

  @Override
  public String getSQLTypeName() throws SQLException {
    return null;
  }

  @Override
  public void readSQL(SQLInput sqlInput, String typeName) throws SQLException {
    string = sqlInput.readString();
    if (sqlInput.wasNull()) {
      string = null;
    }
    b = sqlInput.readByte();
    if (sqlInput.wasNull()) {
      b = null;
    }
    s = sqlInput.readShort();
    if (sqlInput.wasNull()) {
      s = null;
    }
    i = sqlInput.readInt();
    if (sqlInput.wasNull()) {
      i = null;
    }
    l = sqlInput.readLong();
    if (sqlInput.wasNull()) {
      l = null;
    }
    f = sqlInput.readFloat();
    if (sqlInput.wasNull()) {
      f = null;
    }
    d = sqlInput.readDouble();
    if (sqlInput.wasNull()) {
      d = null;
    }
    bd = sqlInput.readBigDecimal();
    bool = sqlInput.readBoolean();
    if (sqlInput.wasNull()) {
      bool = null;
    }
    timestampLtz = sqlInput.readTimestamp();
    timestampNtz = sqlInput.readTimestamp();
    timestampTz = sqlInput.readTimestamp();
    date = sqlInput.readDate();
    time = sqlInput.readTime();
    binary = sqlInput.readBytes();
    simpleClass = sqlInput.readObject(SimpleClass.class);
  }

  @Override
  public void writeSQL(SQLOutput stream) throws SQLException {
    stream.writeString(string);
    stream.writeByte(b);
    stream.writeShort(s);
    stream.writeInt(i);
    stream.writeLong(l);
    stream.writeFloat(f);
    stream.writeDouble(d);
    stream.writeBigDecimal(bd);
    stream.writeBoolean(bool);
    stream.writeTimestamp(timestampLtz);
    stream.writeTimestamp(timestampNtz);
    stream.writeTimestamp(timestampTz);
    stream.writeDate(date);
    stream.writeTime(time);
    stream.writeBytes(binary);
    stream.writeObject(simpleClass);
  }

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
