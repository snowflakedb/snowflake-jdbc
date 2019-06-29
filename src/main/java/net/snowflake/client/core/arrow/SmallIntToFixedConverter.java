/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
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

  public SmallIntToFixedConverter(ValueVector fieldVector, DataConversionContext context)
  {
    super(String.format("%s(%s,%s)", SnowflakeType.FIXED,
                        fieldVector.getField().getMetadata().get("precision"),
                        fieldVector.getField().getMetadata().get("scale")),
          fieldVector,
          context);
    this.smallIntVector = (SmallIntVector) fieldVector;
    String scaleStr = fieldVector.getField().getMetadata().get("scale");
    this.sfScale = Integer.parseInt(scaleStr);
  }

  @Override
  public byte toByte(int index) throws SFException
  {
    short shortVal = toShort(index);
    byte byteVal = (byte) shortVal;

    if (byteVal == shortVal)
    {
      return byteVal;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
                            "byte", shortVal);
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
      short val =
          smallIntVector.getDataBuffer().getShort(index * SmallIntVector.TYPE_WIDTH);
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
                            "int", val);
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
  public Object toObject(int index) throws SFException
  {
    return isNull(index) ? null :
           (sfScale == 0 ? toShort(index) : toBigDecimal(index));
  }

  @Override
  public String toString(int index)
  {
    return isNull(index) ? null : toBigDecimal(index).toString();
  }
}
