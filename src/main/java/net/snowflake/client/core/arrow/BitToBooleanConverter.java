/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Convert Arrow BitVector to Boolean
 */
public class BitToBooleanConverter extends AbstractArrowVectorConverter
{
  private BitVector bitVector;

  public BitToBooleanConverter(ValueVector fieldVector, int columnIndex, DataConversionContext context)
  {
    super(SnowflakeType.BOOLEAN.name(), fieldVector, columnIndex, context);
    this.bitVector = (BitVector) fieldVector;
  }

  private int getBit(int index) {
    // read a bit from the bitVector
    // first find the byte value
    final int byteIndex = index >> 3;
    final byte b = bitVector.getDataBuffer().getByte(byteIndex);
    // then get the bit value
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

  @Override
  public boolean toBoolean(int index)
  {
    if (isNull(index))
    {
      return false;
    }
    else
    {
      return getBit(index) != 0;
    }
  }

  @Override
  public Object toObject(int index)
  {
    return toBoolean(index);
  }

  @Override
  public String toString(int index)
  {
    return isNull(index) ? null : String.valueOf(toBoolean(index));
  }
}
