/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;

public class TinyIntToBooleanConverter extends AbstractArrowVectorConverter
{
  private TinyIntVector tinyIntVector;

  public TinyIntToBooleanConverter(ValueVector fieldVector)
  {
    super(SnowflakeType.BOOLEAN, fieldVector);
    this.tinyIntVector = (TinyIntVector) fieldVector;
  }

  @Override
  public boolean toBoolean(int index)
  {
    if (tinyIntVector.isNull(index))
    {
      return false;
    }
    else
    {
      return tinyIntVector.getDataBuffer().getBoolean(
          index * TinyIntVector.TYPE_WIDTH);
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
    return String.valueOf(toBoolean(index));
  }
}
