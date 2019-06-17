/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

import java.math.BigDecimal;

public class IntToFixedConverter extends AbstractArrowVectorConverter
{
  private IntVector intVector;

  private Integer sfScale;

  public IntToFixedConverter(ValueVector fieldVector)
  {
    super(String.format("%s(%s,%s)", SnowflakeType.FIXED,
                        fieldVector.getField().getMetadata().get("precision"),
                        fieldVector.getField().getMetadata().get("scale")),
          fieldVector);
    this.intVector = (IntVector) fieldVector;
    String scaleStr = fieldVector.getField().getMetadata().get("scale");
    this.sfScale = Integer.parseInt(scaleStr);
  }

  @Override
  public byte toByte(int index) throws SFException
  {
    int val = toInt(index);

    if (val >> 8 == 0)
    {
      return (byte) val;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
                            "byte", val);
    }
  }

  @Override
  public short toShort(int index) throws SFException
  {
    int val = toInt(index);

    if (val >> 16 == 0)
    {
      return (short) val;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
                            "byte", val);
    }
  }

  @Override
  public int toInt(int index) throws SFException
  {
    if (intVector.isNull(index))
    {
      return 0;
    }
    else if (sfScale != 0)
    {
      int val = intVector.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
                            "int", val);
    }
    else
    {
      return intVector.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);
    }
  }

  @Override
  public long toLong(int index) throws SFException
  {
    return (long) toInt(index);
  }

  @Override
  public BigDecimal toBigDecimal(int index)
  {
    if (intVector.isNull(index))
    {
      return null;
    }
    else
    {
      int val = intVector.getDataBuffer().getInt(
          index * IntVector.TYPE_WIDTH);
      return BigDecimal.valueOf((long) val, sfScale);
    }
  }

  @Override
  public BigDecimal toBigDecimal(int index, int scale)
  {
    if (intVector.isNull(index))
    {
      return null;
    }
    else
    {
      int val = intVector.getDataBuffer().getInt(
          index * IntVector.TYPE_WIDTH);
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
