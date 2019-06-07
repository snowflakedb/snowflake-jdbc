/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;

import java.math.BigDecimal;

/**
 * A converter from arrow tinyint to Snowflake Fixed type converter
 */
public class TinyIntToFixedConverter extends AbstractArrowVectorConverter
{
  private TinyIntVector tinyIntVector;

  private Integer sfScale;

  public TinyIntToFixedConverter(ValueVector fieldVector)
  {
    super(SnowflakeType.FIXED, fieldVector);
    this.tinyIntVector = (TinyIntVector) fieldVector;
    String scaleStr = fieldVector.getField().getMetadata().get("scale");
    this.sfScale = Integer.parseInt(scaleStr);
  }

  @Override
  public byte toByte(int index) throws SFException
  {
    if (tinyIntVector.isNull(index))
    {
      return 0;
    }
    else if (sfScale != 0)
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalType.name(),
                            "byte");
    }
    else
    {
      return tinyIntVector.getDataBuffer().getByte(
          index * TinyIntVector.TYPE_WIDTH);
    }
  }

  @Override
  public short toShort(int index) throws SFException
  {
    return (short) toByte(index);
  }

  @Override
  public int toInt(int index) throws SFException
  {
    return (int) toByte(index);
  }

  @Override
  public long toLong(int index) throws SFException
  {
    return (long) toByte(index);
  }

  @Override
  public BigDecimal toBigDecimal(int index)
  {
    if (tinyIntVector.isNull(index))
    {
      return null;
    }
    else
    {
      byte val = tinyIntVector.getDataBuffer().getByte(
          index * TinyIntVector.TYPE_WIDTH);
      return BigDecimal.valueOf((long) val, sfScale);
    }
  }

  @Override
  public BigDecimal toBigDecimal(int index, int scale)
  {
    if (tinyIntVector.isNull(index))
    {
      return null;
    }
    else
    {
      byte val = tinyIntVector.getDataBuffer().getByte(
          index * TinyIntVector.TYPE_WIDTH);
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
