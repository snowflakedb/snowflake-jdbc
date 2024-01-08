package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Iterator;
import net.snowflake.client.core.structs.SQLDataCreationHelper;
import net.snowflake.client.jdbc.SnowflakeLoggedFeatureNotSupportedException;

// TODO structuredType use json converters
public class JsonSqlInput implements SQLInput {
  private final JsonNode input;
  private final Iterator<JsonNode> elements;
  private final SFBaseSession session;

  public JsonSqlInput(JsonNode input, SFBaseSession session) {
    this.input = input;
    this.elements = input.elements();
    this.session = session;
  }

  public JsonNode getInput() {
    return input;
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
    return elements.next().decimalValue();
  }

  @Override
  public byte[] readBytes() throws SQLException {
    // TODO structuredType use converters, currently only invoking `next` to move iterator forward
    elements.next();
    return new byte[0];
  }

  @Override
  public Date readDate() throws SQLException {
    // TODO structuredType use converters, currently only invoking `next` to move iterator forward
    elements.next();
    return null;
  }

  @Override
  public Time readTime() throws SQLException {
    // TODO structuredType use converters, currently only invoking `next` to move iterator forward
    elements.next();
    return null;
  }

  @Override
  public Timestamp readTimestamp() throws SQLException {
    // TODO structuredType use converters, currently only invoking `next` to move iterator forward
    elements.next();
    return null;
  }

  @Override
  public Reader readCharacterStream() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readCharacterStream");
  }

  @Override
  public InputStream readAsciiStream() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readAsciiStream");
  }

  @Override
  public InputStream readBinaryStream() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readBinaryStream");
  }

  @Override
  public Object readObject() throws SQLException {
    // TODO structuredType return map
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readCharacterStream");
  }

  @Override
  public Ref readRef() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readRef");
  }

  @Override
  public Blob readBlob() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readBlob");
  }

  @Override
  public Clob readClob() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readClob");
  }

  @Override
  public Array readArray() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readArray");
  }

  @Override
  public boolean wasNull() throws SQLException {
    return false; // nulls are not allowed in structure types
  }

  @Override
  public URL readURL() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readCharacterStream");
  }

  @Override
  public NClob readNClob() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readNClob");
  }

  @Override
  public String readNString() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readNString");
  }

  @Override
  public SQLXML readSQLXML() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readSQLXML");
  }

  @Override
  public RowId readRowId() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readRowId");
  }

  @Override
  public <T> T readObject(Class<T> type) throws SQLException {
    JsonNode jsonNode = elements.next();
    SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
    instance.readSQL(new JsonSqlInput(jsonNode, session), null);
    return (T) instance;
  }
}
