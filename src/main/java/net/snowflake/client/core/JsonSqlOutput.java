package net.snowflake.client.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.core.structs.SFSqlOutput;

import java.util.*;

public class JsonSqlOutput implements SFSqlOutput {

  private Map attribs = new HashMap();
  private final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();

  @Override
  public void writeString(String fieldName, String value) {
    attribs.put(fieldName, value);
  }

  @Override
  public void writeObject(String fieldName, Object value) {
      attribs.put(fieldName, value);
  }

  public String getJsonString() {
    try {
      return OBJECT_MAPPER.writeValueAsString(attribs);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}

