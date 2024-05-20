/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import java.util.List;
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
    return StructuredTypeConversionHelper.mapHashMapToObject(entriesList);
  }

  @Override
  public String toString(int index) throws SFException {
    return StructuredTypeConversionHelper.mapJson(toObject(index));
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    return toString(index).getBytes();
  }
}
