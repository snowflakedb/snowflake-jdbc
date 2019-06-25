/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

import java.sql.Date;
import java.util.TimeZone;

public class IntToDateConverter extends AbstractArrowVectorConverter
{
  private IntVector intVector;

  /**
   * date data formatter
   */
  private SnowflakeDateTimeFormat dateFormatter;
  private TimeZone defaultTimeZone;

  public IntToDateConverter(ValueVector fieldVector,
                            SnowflakeDateTimeFormat dateFormatter)
  {
    super(SnowflakeType.DATE.name(), fieldVector);
    this.intVector = (IntVector) fieldVector;
    this.dateFormatter = dateFormatter;
    this.defaultTimeZone = TimeZone.getDefault();
  }

  @Override
  public Date toDate(int index) throws SFException
  {
    if (intVector.isNull(index))
    {
      return null;
    }
    else
    {
      int val = intVector.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);
      return ResultUtil.getDate(val, defaultTimeZone);
    }
  }

  @Override
  public int toInt(int index)
  {
    if (intVector.isNull(index))
    {
      return 0;
    }
    else
    {
      int val = intVector.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);
      return val;
    }
  }

  @Override
  public String toString(int index) throws SFException
  {
    return isNull(index) ? null : ResultUtil.getDateAsString(toDate(index), dateFormatter);
  }

  @Override
  public Object toObject(int index) throws SFException
  {
    return isNull(index) ? null : toDate(index);
  }
}
