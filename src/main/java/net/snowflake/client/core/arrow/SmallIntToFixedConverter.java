/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.ValueVector;

import java.math.BigDecimal;

public class SmallIntToFixedConverter extends AbstractArrowVectorConverter
{
  private SmallIntVector smallIntVector;

  private Integer sfScale;

  public SmallIntToFixedConverter(ValueVector fieldVector)
  {
    super(SnowflakeType.FIXED, fieldVector);
    this.smallIntVector = (SmallIntVector) fieldVector;
    String scaleStr = fieldVector.getField().getMetadata().get("scale");
    this.sfScale = Integer.parseInt(scaleStr);
  }

  @Override
  public byte toByte(int index) throws SFException
  {
    short val = toShort(index);

    if (val >> 8 == 0)
    {
      return (byte) val;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalType.name(),
                            "byte", val);
    }
  }

  @Override
  public short toShort(int index) throws SFException
  {
    if (smallIntVector.isNull(index))
    {
      return 0;
    }
    else if (sfScale != 0)
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalType.name(),
                            "int");
    }
    else
    {
      return smallIntVector.getDataBuffer().getShort(index * SmallIntVector.TYPE_WIDTH);
    }
  }

  @Override
  public int toInt(int index) throws SFException
  {
    return (int) toShort(index);
  }

  @Override
  public long toLong(int index) throws SFException
  {
    return (long) toShort(index);
  }

  @Override
  public BigDecimal toBigDecimal(int index)
  {
    if (smallIntVector.isNull(index))
    {
      return null;
    }
    else
    {
      short val = smallIntVector.getDataBuffer().getShort(
          index * SmallIntVector.TYPE_WIDTH);
      return BigDecimal.valueOf((long) val, sfScale);
    }
  }

  @Override
  public BigDecimal toBigDecimal(int index, int scale)
  {
    if (smallIntVector.isNull(index))
    {
      return null;
    }
    else
    {
      short val = smallIntVector.getDataBuffer().getShort(
          index * SmallIntVector.TYPE_WIDTH);
      return BigDecimal.valueOf((long) val, scale);
    }
  }

  @Override
  public Object toObject(int index)
  {
    return toBigDecimal(index);
  }

  @Override
  public String toString(int index)
  {
    return toBigDecimal(index).toString();
  }
}
