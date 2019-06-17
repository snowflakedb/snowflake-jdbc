/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

import java.sql.Date;

public class IntToDateConverter extends AbstractArrowVectorConverter
{
  private IntVector intVector;

  /**
   * date data formatter
   */
  private SnowflakeDateTimeFormat dateFormatter;

  public IntToDateConverter(ValueVector fieldVector,
                            SnowflakeDateTimeFormat dateFormatter)
  {
    super(SnowflakeType.DATE.name(), fieldVector);
    this.intVector = (IntVector) fieldVector;
    this.dateFormatter = dateFormatter;
  }

  @Override
  public Date toDate(int index)
  {
    if (intVector.isNull(index))
    {
      return null;
    }
    else
    {
      int val = intVector.getDataBuffer().getInt(index);
      return new Date(val * 86400000);
    }
  }

  @Override
  public String toString(int index)
  {
    return toDate(index).toString();
  }

  @Override
  public Object toObject(int index)
  {
    return toDate(index);
  }
}
