package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.Iterator;
import net.snowflake.client.core.structs.SQLDataCreationHelper;

// TODO structuredType use json converters
public class JsonSqlInput implements SQLInput {
  public JsonNode getInput() {
    return input;
  }

  private final JsonNode input;

  private final Iterator<JsonNode> elements;

  public JsonSqlInput(JsonNode input) {
    this.input = input;
    this.elements = input.elements();
  }

  @Override
  public String readString() throws SQLException {
    return elements.next().textValue();
  }

  @Override
  public boolean readBoolean() throws SQLException {
    return elements.next().booleanValue();
  }

  @Override
  public byte readByte() throws SQLException {
    return elements.next().numberValue().byteValue();
  }

  @Override
  public short readShort() throws SQLException {
    return elements.next().numberValue().shortValue();
  }

  @Override
  public int readInt() throws SQLException {
    return elements.next().numberValue().intValue();
  }

  @Override
  public long readLong() throws SQLException {
    return elements.next().numberValue().longValue();
  }

  @Override
  public float readFloat() throws SQLException {
    return elements.next().numberValue().floatValue();
  }

  @Override
  public double readDouble() throws SQLException {
    return elements.next().numberValue().doubleValue();
  }

  @Override
  public BigDecimal readBigDecimal() throws SQLException {
    return null;
  }

  @Override
  public byte[] readBytes() throws SQLException {
    return new byte[0];
  }

  @Override
  public Date readDate() throws SQLException {
    return null;
  }

  @Override
  public Time readTime() throws SQLException {
    return null;
  }

  @Override
  public Timestamp readTimestamp() throws SQLException {
    return null;
  }

  @Override
  public Reader readCharacterStream() throws SQLException {
    return null;
  }

  @Override
  public InputStream readAsciiStream() throws SQLException {
    return null;
  }

  @Override
  public InputStream readBinaryStream() throws SQLException {
    return null;
  }

  @Override
  public Object readObject() throws SQLException {
    return input.elements().next();
  }

  @Override
  public Ref readRef() throws SQLException {
    return null;
  }

  @Override
  public Blob readBlob() throws SQLException {
    return null;
  }

  @Override
  public Clob readClob() throws SQLException {
    return null;
  }

  @Override
  public Array readArray() throws SQLException {
    return null;
  }

  @Override
  public boolean wasNull() throws SQLException {
    return false;
  }

  @Override
  public URL readURL() throws SQLException {
    return null;
  }

  @Override
  public NClob readNClob() throws SQLException {
    return null;
  }

  @Override
  public String readNString() throws SQLException {
    return null;
  }

  @Override
  public SQLXML readSQLXML() throws SQLException {
    return null;
  }

  @Override
  public RowId readRowId() throws SQLException {
    return null;
  }

  @Override
  public <T> T readObject(Class<T> type) throws SQLException {
    JsonNode jsonNode = elements.next();
    SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
    instance.readSQL(new JsonSqlInput(jsonNode), null);
    return (T) instance;
  }
}
