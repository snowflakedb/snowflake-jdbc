/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;

import java.math.BigDecimal;

/**
 * Data vector whose snowflake logical type is fixed while represented as a
 * long value vector
 */
public class BigIntToFixedConverter extends AbstractArrowVectorConverter
{
  /**
   * Underlying vector that this converter will convert from
   */
  private BigIntVector bigIntVector;

  /**
   * scale of the fixed value
   */
  private Integer sfScale;

  public BigIntToFixedConverter(ValueVector fieldVector, int columnIndex, DataConversionContext context)
  {
    super(String.format("%s(%s,%s)", SnowflakeType.FIXED,
                        fieldVector.getField().getMetadata().get("precision"),
                        fieldVector.getField().getMetadata().get("scale")),
          fieldVector,
          columnIndex,
          context);
    this.bigIntVector = (BigIntVector) fieldVector;
    String scaleStr = fieldVector.getField().getMetadata().get("scale");
    this.sfScale = Integer.parseInt(scaleStr);
  }

  @Override
  public byte toByte(int index) throws SFException
  {
    long longVal = toLong(index);
    byte byteVal = (byte) longVal;

    if (byteVal == longVal)
    {
      return byteVal;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                            logicalTypeStr,
                            "byte", longVal);
    }
  }

  @Override
  public short toShort(int index) throws SFException
  {
    long longVal = toLong(index);
    short shortVal = (short) longVal;

    if (shortVal == longVal)
    {
      return shortVal;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                            logicalTypeStr,
                            "byte", longVal);
    }
  }

  @Override
  public int toInt(int index) throws SFException
  {
    long longVal = toLong(index);
    int intVal = (int) longVal;

    if (intVal == longVal)
    {
      return intVal;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                            logicalTypeStr,
                            "byte", longVal);
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
      long val =
          bigIntVector.getDataBuffer().getLong(index * BigIntVector.TYPE_WIDTH);
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT,
                            logicalTypeStr,
                            "long",
                            val);
    }
    else
    {
      return bigIntVector.getDataBuffer().getLong(index * BigIntVector.TYPE_WIDTH);
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
      long val = bigIntVector.getDataBuffer().getLong(
          index * BigIntVector.TYPE_WIDTH);
      return BigDecimal.valueOf(val, sfScale);
    }
  }

  @Override
  public Object toObject(int index) throws SFException
  {
    return isNull(index) ? null :
           (sfScale == 0 ? toLong(index) : toBigDecimal(index));
  }

  @Override
  public String toString(int index)
  {
    return isNull(index) ? null : toBigDecimal(index).toString();
  }
}
