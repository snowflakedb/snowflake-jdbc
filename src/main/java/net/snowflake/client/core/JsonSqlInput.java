package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.*;

import java.sql.SQLException;

public class JsonSqlInput implements SqlInput {
  private final JsonNode input;

  public JsonSqlInput(JsonNode input) {
    this.input = input;
  }

  @Override
  public String readString(String fieldName) throws SQLException {
    return input.get(fieldName).textValue();
  }

  @Override
  public Byte readByte(String fieldName) {
    return input.get(fieldName).numberValue().byteValue();
  }

  @Override
  public Short readShort(String fieldName) {
    return input.get(fieldName).shortValue();
  }

  @Override
  public Integer readInt(String fieldName) {
    return input.get(fieldName).intValue();
  }

  @Override
  public Long readLong(String fieldName) {
    return input.get(fieldName).longValue();
  }

  @Override
  public Float readFloat(String fieldName) {
    return input.get(fieldName).floatValue();
  }

  @Override
  public Double readDouble(String fieldName) {
    return input.get(fieldName).doubleValue();
  }

  @Override
  public Boolean readBoolean(String fieldName) throws SQLException {
    return input.get(fieldName).booleanValue();
  }
}
