/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;

import java.math.BigDecimal;

public class BigIntToFixedConverter extends AbstractArrowVectorConverter
{
  private BigIntVector bigIntVector;

  private Integer sfScale;

  public BigIntToFixedConverter(ValueVector fieldVector)
  {
    super(SnowflakeType.FIXED, fieldVector);
    this.bigIntVector = (BigIntVector) fieldVector;
    String scaleStr = fieldVector.getField().getMetadata().get("scale");
    this.sfScale = Integer.parseInt(scaleStr);
  }

  @Override
  public byte toByte(int index) throws SFException
  {
    long val = toLong(index);

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
    long val = toLong(index);

    if (val >> 16 == 0)
    {
      return (short) val;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalType.name(),
                            "byte", val);
    }
  }

  @Override
  public int toInt(int index) throws SFException
  {
    long val = toLong(index);

    if (val >> 32 == 0)
    {
      return (int) val;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalType.name(),
                            "byte", val);
    }
  }

  @Override
  public long toLong(int index) throws SFException
  {
    if (bigIntVector.isNull(index))
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
      return bigIntVector.getDataBuffer().getShort(index * BigIntVector.TYPE_WIDTH);
    }
  }

  @Override
  public BigDecimal toBigDecimal(int index)
  {
    if (bigIntVector.isNull(index))
    {
      return null;
    }
    else
    {
      long val = bigIntVector.getDataBuffer().getByte(
          index * BigIntVector.TYPE_WIDTH);
      return BigDecimal.valueOf(val, sfScale);
    }
  }

  @Override
  public BigDecimal toBigDecimal(int index, int scale)
  {
    if (bigIntVector.isNull(index))
    {
      return null;
    }
    else
    {
      long val = bigIntVector.getDataBuffer().getByte(
          index * BigIntVector.TYPE_WIDTH);
      return BigDecimal.valueOf(val, scale);
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
