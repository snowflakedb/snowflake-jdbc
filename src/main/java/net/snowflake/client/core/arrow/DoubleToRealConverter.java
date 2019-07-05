/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;

public class DoubleToRealConverter extends AbstractArrowVectorConverter
{
  private Float8Vector float8Vector;

  public DoubleToRealConverter(ValueVector fieldVector, int columnIndex, DataConversionContext context)
  {
    super(SnowflakeType.REAL.name(), fieldVector, columnIndex, context);
    this.float8Vector = (Float8Vector) fieldVector;
  }

  @Override
  public double toDouble(int index)
  {
    if (float8Vector.isNull(index))
    {
      return 0;
    }
    else
    {
      return float8Vector.getDataBuffer().getDouble(index * Float8Vector.TYPE_WIDTH);
    }
  }

  @Override
  public float toFloat(int index)
  {
    return (float) toDouble(index);
  }

  @Override
  public Object toObject(int index)
  {
    return isNull(index) ? null : toDouble(index);
  }

  @Override
  public String toString(int index)
  {
    return isNull(index) ? null : String.valueOf(toDouble(index));
  }
}
