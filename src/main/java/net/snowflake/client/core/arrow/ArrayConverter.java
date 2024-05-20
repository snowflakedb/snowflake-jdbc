/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import java.nio.charset.StandardCharsets;
import java.util.List;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.complex.ListVector;

public class ArrayConverter extends AbstractArrowVectorConverter {

  private final ListVector vector;

  public ArrayConverter(ListVector valueVector, int vectorIndex, DataConversionContext context) {
    super(SnowflakeType.ARRAY.name(), valueVector, vectorIndex, context);
    this.vector = valueVector;
  }

  @Override
  public Object toObject(int index) throws SFException {
    List<?> list = vector.getObject(index);
    return StructuredTypeConversionHelper.mapListToObject(list);
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
