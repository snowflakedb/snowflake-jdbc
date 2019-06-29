/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;

import java.nio.charset.StandardCharsets;

public class VarCharToTextConverter extends AbstractArrowVectorConverter
{
  private VarCharVector varCharVector;

  public VarCharToTextConverter(ValueVector valueVector, DataConversionContext context)
  {
    super(SnowflakeType.TEXT.name(), valueVector, context);
    this.varCharVector = (VarCharVector) valueVector;
  }

  @Override
  public String toString(int index)
  {
    byte[] bytes = toBytes(index);
    return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public byte[] toBytes(int index)
  {
    return isNull(index) ? null : varCharVector.get(index);
  }

  @Override
  public Object toObject(int index)
  {
    return toString(index);
  }
}
