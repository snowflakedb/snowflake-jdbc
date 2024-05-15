/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.complex.ListVector;

import java.util.List;

public class ArrayConverter extends AbstractArrowVectorConverter {

  private final ListVector vector;
  private static final ObjectMapper objectMapper = ObjectMapperFactory.getObjectMapper();

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
