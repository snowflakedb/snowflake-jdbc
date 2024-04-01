package net.snowflake.client.core.arrow;

import java.util.List;
import java.util.stream.Collectors;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.util.JsonStringHashMap;

@SnowflakeJdbcInternalApi
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
    return entriesList.stream()
        .collect(
            Collectors.toMap(entry -> entry.get("key").toString(), entry -> entry.get("value")));
  }

  @Override
  public String toString(int index) throws SFException {
    return vector.getObject(index).toString();
  }
}
