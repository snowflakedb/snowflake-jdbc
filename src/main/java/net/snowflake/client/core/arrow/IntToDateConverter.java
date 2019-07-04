/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.IncidentUtil;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.TimeZone;

public class IntToDateConverter extends AbstractArrowVectorConverter
{
  private IntVector intVector;

  public IntToDateConverter(ValueVector fieldVector, int columnIndex, DataConversionContext context)
  {
    super(SnowflakeType.DATE.name(), fieldVector, columnIndex, context);
    this.intVector = (IntVector) fieldVector;
  }

  @Override
  public Date toDate(int index) throws SFException
  {
    if (isNull(index))
    {
      return null;
    }
    else
    {
      int val = intVector.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);
      // Note: use default time zone to match with current getDate() behavior
      return ArrowResultUtil.getDate(val, TimeZone.getDefault(), context.getSession());
    }
  }

  @Override
  public int toInt(int index)
  {
    if (isNull(index))
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
  public Timestamp toTimestamp(int index, TimeZone tz) throws SFException
  {
    if (isNull(index))
    {
      return null;
    }
    else
    {
      return new Timestamp(toDate(index).getTime());
    }
  }

  @Override
  public String toString(int index) throws SFException
  {
    if (context.getDateFormatter() == null)
    {
      throw (SFException) IncidentUtil.generateIncidentV2WithException(
          context.getSession(),
          new SFException(ErrorCode.INTERNAL_ERROR,
                          "missing date formatter"),
          null,
          null);
    }
    return isNull(index) ? null :
           ResultUtil.getDateAsString(toDate(index), context.getDateFormatter());
  }

  @Override
  public Object toObject(int index) throws SFException
  {
    return isNull(index) ? null : toDate(index);
  }
}
