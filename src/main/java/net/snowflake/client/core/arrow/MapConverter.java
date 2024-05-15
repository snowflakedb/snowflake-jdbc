package net.snowflake.client.core.arrow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.stream.Collectors;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.util.JsonStringHashMap;

public class MapConverter extends AbstractArrowVectorConverter {

  private final MapVector vector;
  private ObjectMapper objectMapper;

  public MapConverter(MapVector valueVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.MAP.name(), valueVector, columnIndex, context);
    this.vector = valueVector;
    this.objectMapper = ObjectMapperFactory.getObjectMapper();
  }

  @Override
  public Object toObject(int index) throws SFException {
    List<JsonStringHashMap<String, Object>> entriesList =
        (List<JsonStringHashMap<String, Object>>) vector.getObject(index);
    return mapHashMapToObject(entriesList);
  }

  private Object mapHashMapToObject(List<JsonStringHashMap<String, Object>> entriesList)
      throws SFException {
    return entriesList.stream()
        .collect(
            Collectors.toMap(
                entry -> entry.get("key").toString(),
                entry -> {
                  Object value = entry.get("value");
                  if (value instanceof List) {
                    List<?> list = (List) value;
                    return mapListToObject(list);
                  }
                  return value;
                }));
  }

  private Object mapListToObject(List<?> list) {
    if (list.stream().anyMatch(nested -> nested instanceof JsonStringHashMap)) {
      try {
        return mapHashMapToObject((List<JsonStringHashMap<String, Object>>) list);
      } catch (SFException e) {
        throw new RuntimeException(e);
      }
    } else {
      return list.stream()
          .map(
              nested -> {
                if (nested instanceof List) {
                  return mapListToObject((List<?>) nested);
                } else {
                  return nested;
                }
              })
          .collect(Collectors.toList());
    }
  }

  @Override
  public String toString(int index) throws SFException {
    try {
      return objectMapper.writeValueAsString(toObject(index));
    } catch (JsonProcessingException e) {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, e.getMessage());
    }
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    return toString(index).getBytes();
  }
}
