/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
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

  public IntToFixedConverter(ValueVector fieldVector, int columnIndex, DataConversionContext context)
  {
    super(String.format("%s(%s,%s)", SnowflakeType.FIXED,
                        fieldVector.getField().getMetadata().get("precision"),
                        fieldVector.getField().getMetadata().get("scale")),
          fieldVector,
          columnIndex,
          context);
    this.intVector = (IntVector) fieldVector;
    String scaleStr = fieldVector.getField().getMetadata().get("scale");
    this.sfScale = Integer.parseInt(scaleStr);
  }

  @Override
  public byte toByte(int index) throws SFException
  {
    int intVal = toInt(index);
    byte byteVal = (byte) intVal;

    if (byteVal == intVal)
    {
      return byteVal;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
                            "byte", intVal);
    }
  }

  @Override
  public short toShort(int index) throws SFException
  {
    int intVal = toInt(index);
    short shortVal = (short) intVal;

    if (shortVal == intVal)
    {
      return shortVal;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
                            "byte", intVal);
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
  public Object toObject(int index) throws SFException
  {
    return isNull(index) ? null :
           (sfScale == 0 ? toInt(index) : toBigDecimal(index));
  }

  @Override
  public String toString(int index)
  {
    return isNull(index) ? null : toBigDecimal(index).toString();
  }

}
