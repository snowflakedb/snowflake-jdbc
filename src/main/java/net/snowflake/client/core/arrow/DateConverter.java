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
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.TimeZone;

/** Convert Arrow DateDayVector to Date */
public class DateConverter extends AbstractArrowVectorConverter {
  private DateDayVector dateVector;

  public DateConverter(ValueVector fieldVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.DATE.name(), fieldVector, columnIndex, context);
    this.dateVector = (DateDayVector) fieldVector;
  }

  private Date getDate(int index) throws SFException {
    if (isNull(index)) {
      return null;
    } else {
      int val = dateVector.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);
      // Note: use default time zone to match with current getDate() behavior
      return ArrowResultUtil.getDate(val);
    }
  }

  @Override
  public Date toDate(int index) throws SFException {
    return getDate(index);
  }

  @Override
  public int toInt(int index) {
    if (isNull(index)) {
      return 0;
    } else {
      int val = dateVector.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);
      return val;
    }
  }

  @Override
  public short toShort(int index) throws SFException {
    try {
      return (short) toInt(index);
    } catch (ClassCastException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.SHORT_STR, toInt(index));
    }
  }

  @Override
  public long toLong(int index) {
    return toInt(index);
  }

  @Override
  public float toFloat(int index) {
    return toInt(index);
  }

  @Override
  public double toDouble(int index) {
    return toInt(index);
  }

  @Override
  public BigDecimal toBigDecimal(int index) {
    if (isNull(index)) {
      return null;
    }
    return BigDecimal.valueOf(toInt(index));
  }

  @Override
  public Timestamp toTimestamp(int index, TimeZone tz) throws SFException {
    Date date = toDate(index);
    if (date == null) {
      return null;
    } else {
      return new Timestamp(date.getTime());
    }
  }

  @Override
  public String toString(int index) throws SFException {
    if (context.getDateFormatter() == null) {
      throw (SFException)
          IncidentUtil.generateIncidentV2WithException(
              context.getSession(),
              new SFException(ErrorCode.INTERNAL_ERROR, "missing date formatter"),
              null,
              null);
    }
    Date date = getDate(index);
    return date == null ? null : ResultUtil.getDateAsString(date, context.getDateFormatter());
  }

  @Override
  public Object toObject(int index) throws SFException {
    return toDate(index);
  }

  @Override
  public boolean toBoolean(int index) throws SFException {
    if (isNull(index)) {
      return false;
    }
    Date val = toDate(index);
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
        SnowflakeUtil.BOOLEAN_STR, val);
  }
}
