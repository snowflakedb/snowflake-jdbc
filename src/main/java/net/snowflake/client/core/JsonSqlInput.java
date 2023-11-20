package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.core.structs.SFSqlData;
import net.snowflake.client.core.structs.SFSqlDataCreationHelper;
import net.snowflake.client.core.structs.SFSqlInput;

import java.sql.SQLException;

public class JsonSqlInput implements SFSqlInput {
  public JsonNode getInput() {
    return input;
  }

  private final JsonNode input;

  public JsonSqlInput(JsonNode input) {
    this.input = input;
  }

  @Override
  public String readString(String fieldName) throws SQLException {
    return input.get(fieldName).textValue();
  }

  @Override
  public Boolean readBoolean(String fieldName) throws SQLException {
    return input.get(fieldName).booleanValue();
  }

  @Override
  public <T extends SFSqlData> T readObject(String fieldName, Class<T> type) throws SQLException {
    JsonNode jsonNode = input.get(fieldName);
    T instance = SFSqlDataCreationHelper.create(type);
    instance.readSql(new JsonSqlInput(jsonNode));
    return instance;
  }
}
