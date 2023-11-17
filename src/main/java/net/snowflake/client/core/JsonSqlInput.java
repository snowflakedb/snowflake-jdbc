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
}
