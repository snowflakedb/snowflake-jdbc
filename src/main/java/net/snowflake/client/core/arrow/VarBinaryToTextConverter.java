/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;

import java.nio.charset.StandardCharsets;

public class VarBinaryToTextConverter extends AbstractArrowVectorConverter
{
  private VarBinaryVector varBinaryVector;

  public VarBinaryToTextConverter(ValueVector valueVector)
  {
    super(SnowflakeType.TEXT, valueVector);
    this.varBinaryVector = (VarBinaryVector) valueVector;
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
    return varBinaryVector.get(index);
  }

  @Override
  public Object toObject(int index)
  {
    return toString(index);
  }
}
