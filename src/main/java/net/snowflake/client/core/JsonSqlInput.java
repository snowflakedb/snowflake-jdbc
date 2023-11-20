package net.snowflake.client.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import net.snowflake.client.core.structs.SFSqlData;
import net.snowflake.client.core.structs.SFSqlDataCreationHelper;
import net.snowflake.client.core.structs.SFSqlInput;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

  @Override
  public <T extends SFSqlData> T readObject(String fieldName, Class<T> type) throws SQLException {
    JsonNode jsonNode = input.get(fieldName);
    T instance = SFSqlDataCreationHelper.create(type);
    instance.readSql(new JsonSqlInput(jsonNode));
    return instance;
  }

  public <T extends SFSqlData> List<T> readList(String fieldName, Class<T> type) throws SQLException {
    List<T> resultList = new ArrayList<>();
      ArrayNode arrayNode = (ArrayNode) input.get(fieldName);
      Iterator nodeElements = arrayNode.elements();
      while (nodeElements.hasNext()) {
        T instance = SFSqlDataCreationHelper.create(type);
        instance.readSql(new JsonSqlInput((JsonNode) nodeElements.next()));
        resultList.add(instance);
      }
      return resultList;
  }

  public <T extends SFSqlData> T[] readArray(String fieldName, Class<T> type) throws SQLException {
    ArrayNode arrayNode = (ArrayNode) input.get(fieldName);
    T[] arr = (T[]) java.lang.reflect.Array.newInstance(type, arrayNode.size());
    AtomicInteger counter = new AtomicInteger(0);
    Iterator nodeElements = arrayNode.elements();
    while (nodeElements.hasNext()) {
      T instance = SFSqlDataCreationHelper.create(type);
      instance.readSql(new JsonSqlInput((JsonNode) nodeElements.next()));
      arr[counter.getAndIncrement()] = (T) instance;
    }
    return arr;
  }

  @Override
  public <K, T extends SFSqlData> Map<K, T> readMap(String fieldName, Class<K> keyType, Class<T> type) throws SQLException {
    ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();
    JsonNode jsonNode = input.get(fieldName);
    Map<Object, Object> map = OBJECT_MAPPER.convertValue(jsonNode, new TypeReference<Map<Object, Object>>() {});
    Map<K, T> collect = map.entrySet().stream().map(e -> {
              try {
                T instance = SFSqlDataCreationHelper.create(type);
                SFSqlInput sqlInput = new JsonSqlInput(jsonNode.get(e.getKey().toString()));
                instance.readSql(sqlInput);
                return new AbstractMap.SimpleEntry<>((K)e.getKey(), (T) instance);
              } catch (SQLException ex) {
                throw new RuntimeException(ex);
              }
            })
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue
            ));
    return collect;
  }
}
