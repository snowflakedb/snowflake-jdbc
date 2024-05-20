/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.complex.StructVector;

@SnowflakeJdbcInternalApi
public class StructConverter extends AbstractArrowVectorConverter {

  private final StructVector structVector;

  public StructConverter(StructVector vector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.OBJECT.name(), vector, columnIndex, context);
    structVector = vector;
  }

  @Override
  public Object toObject(int index) throws SFException {
    Map<String, ?> object = structVector.getObject(index);
    return StructuredTypeConversionHelper.mapStructToObject(object);
  }

  @Override
  public String toString(int index) throws SFException {
    return StructuredTypeConversionHelper.mapJson(toObject(index));
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    return toString(index).getBytes(StandardCharsets.UTF_8);
  }
}
