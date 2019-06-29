/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.common.core.SFBinary;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;

public class VarBinaryToBinaryConverter extends AbstractArrowVectorConverter
{
  private VarBinaryVector varBinaryVector;

  public VarBinaryToBinaryConverter(ValueVector valueVector, DataConversionContext context)
  {
    super(SnowflakeType.BINARY.name(), valueVector, context);
    this.varBinaryVector = (VarBinaryVector) valueVector;
  }

  @Override
  public String toString(int index)
  {
    byte[] bytes = toBytes(index);
    SFBinary binary = new SFBinary(bytes);
    return bytes == null ? null : context.getBinaryFormatter().format(binary);
  }

  @Override
  public byte[] toBytes(int index)
  {
    return varBinaryVector.getObject(index);
  }

  @Override
  public Object toObject(int index)
  {
    return toBytes(index);
  }
}
