package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.core.structs.SFSqlInput;

import java.sql.SQLException;

public class JsonSqlInput implements SFSqlInput {
  private final JsonNode input;

  public JsonSqlInput(JsonNode input) {
    this.input = input;
  }

  @Override
  public String readString(String fieldName) throws SQLException {
    return input.get(fieldName).textValue();
  }

//  @Override
//  public boolean readBoolean() throws SQLException {
//    return elements.next().booleanValue();
//  }

//  @Override
//  public byte readByte() throws SQLException {
//    return 0;
//  }
//
//  @Override
//  public short readShort() throws SQLException {
//    return 0;
//  }
//
//  @Override
//  public int readInt() throws SQLException {
//    return 0;
//  }
//
//  @Override
//  public long readLong() throws SQLException {
//    return 0;
//  }
//
//  @Override
//  public float readFloat() throws SQLException {
//    return 0;
//  }
//
//  @Override
//  public double readDouble() throws SQLException {
//    return 0;
//  }
//
//  @Override
//  public BigDecimal readBigDecimal() throws SQLException {
//    return null;
//  }
//
//  @Override
//  public byte[] readBytes() throws SQLException {
//    return new byte[0];
//  }
//
//  @Override
//  public Date readDate() throws SQLException {
//    return null;
//  }
//
//  @Override
//  public Time readTime() throws SQLException {
//    return null;
//  }
//
//  @Override
//  public Timestamp readTimestamp() throws SQLException {
//    return null;
//  }
//
//  @Override
//  public Reader readCharacterStream() throws SQLException {
//    return null;
//  }
//
//  @Override
//  public InputStream readAsciiStream() throws SQLException {
//    return null;
//  }
//
//  @Override
//  public InputStream readBinaryStream() throws SQLException {
//    return null;
//  }


  //  TODO extractedType maybe getObject should return raw input then it won't be needed
  public JsonNode getInput() {
    return input;
  }
}
