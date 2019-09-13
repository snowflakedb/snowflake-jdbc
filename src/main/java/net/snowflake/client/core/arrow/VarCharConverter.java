/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

/**
 * Convert Arrow VarCharVector to Java types
 */
public class VarCharConverter extends AbstractArrowVectorConverter
{
  private VarCharVector varCharVector;

  public VarCharConverter(ValueVector valueVector, int columnIndex, DataConversionContext context)
  {
    super(SnowflakeType.TEXT.name(), valueVector, columnIndex, context);
    this.varCharVector = (VarCharVector) valueVector;
  }

  @Override
  public String toString(int index)
  {
    byte[] bytes = toBytes(index);
    return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public byte[] toBytes(int index)
  {
    return isNull(index) ? null : varCharVector.get(index);
  }

  @Override
  public Object toObject(int index)
  {
    return toString(index);
  }

  @Override
  public short toShort(int index)
  {
    String str = toString(index);
    if (str == null)
    {
      return 0;
    }
    else
    {
      return Short.parseShort(str);
    }
  }

  @Override
  public int toInt(int index)
  {
    String str = toString(index);
    if (str == null)
    {
      return 0;
    }
    else
    {
      return Integer.parseInt(str);
    }
  }

  @Override
  public long toLong(int index)
  {
    String str = toString(index);
    if (str == null)
    {
      return 0;
    }
    else
    {
      return Long.parseLong(str);
    }
  }

  @Override
  public float toFloat(int index)
  {
    String str = toString(index);
    if (str == null)
    {
      return 0;
    }
    else
    {
      return Float.parseFloat(str);
    }
  }

  @Override
  public double toDouble(int index)
  {
    String str = toString(index);
    if (str == null)
    {
      return 0;
    }
    else
    {
      return Double.parseDouble(str);
    }
  }

  @Override
  public BigDecimal toBigDecimal(int index)
  {
    String str = toString(index);
    if (str == null)
    {
      return null;
    }
    else
    {
      return new BigDecimal(str);
    }
  }

  @Override
  public boolean toBoolean(int index) throws SFException
  {
    String str = toString(index);
    if (str == null)
    {
      return false;
    }
    else if (str.equals("0"))
    {
      return false;
    }
    else  if (str.equals("1"))
    {
      return true;
    }
    else
    {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
          "Boolean", str);
    }
  }
}
