package net.snowflake.client.core.arrow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.util.JsonStringHashMap;

public class MapConverter extends AbstractArrowVectorConverter {

  private final MapVector vector;

  public MapConverter(MapVector valueVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.MAP.name(), valueVector, columnIndex, context);
    this.vector = valueVector;
  }

  @Override
  public Object toObject(int index) throws SFException {
    List<JsonStringHashMap<String, Object>> entriesList =
        (List<JsonStringHashMap<String, Object>>) vector.getObject(index);
    Map<String, Object> converted = new HashMap<>();
    for (Map map : entriesList) {
      converted.put(map.get("key").toString(), map.get("value"));
    }
    return converted;
  }

  @Override
  public String toString(int index) throws SFException {
    return vector.getObject(index).toString();
  }
}
