/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SFBinaryFormat;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;

public class VarBinaryToBinaryConverter extends AbstractArrowVectorConverter
{
  private VarBinaryVector varBinaryVector;

  private SFBinaryFormat binaryFormat;

  public VarBinaryToBinaryConverter(ValueVector valueVector,
                                    SFBinaryFormat binaryFormat)
  {
    super(SnowflakeType.BINARY.name(), valueVector);
    this.varBinaryVector = (VarBinaryVector) valueVector;
    this.binaryFormat = binaryFormat;
  }

  @Override
  public String toString(int index)
  {
    byte[] bytes = toBytes(index);
    SFBinary binary = new SFBinary(bytes);
    return bytes == null ? null : binaryFormat.format(binary);
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
