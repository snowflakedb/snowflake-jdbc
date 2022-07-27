/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.*;
import net.snowflake.common.core.SFTimestamp;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

/** converter from three-field struct (including epoch, fraction, and timezone) to Timestamp_TZ */
public class ThreeFieldStructToTimestampTZConverter extends AbstractArrowVectorConverter {
  private StructVector structVector;
  private BigIntVector epochs;
  private IntVector fractions;
  private IntVector timeZoneIndices;
  private TimeZone timeZone = TimeZone.getTimeZone("UTC");

  public ThreeFieldStructToTimestampTZConverter(
      ValueVector fieldVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.TIMESTAMP_LTZ.name(), fieldVector, columnIndex, context);
    structVector = (StructVector) fieldVector;
    epochs = structVector.getChild(FIELD_NAME_EPOCH, BigIntVector.class);
    fractions = structVector.getChild(FIELD_NAME_FRACTION, IntVector.class);
    timeZoneIndices = structVector.getChild(FIELD_NAME_TIME_ZONE_INDEX, IntVector.class);
  }

  @Override
  public boolean isNull(int index) {
    return epochs.isNull(index);
  }

  @Override
  public String toString(int index) throws SFException {
    if (context.getTimestampTZFormatter() == null) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "missing timestamp LTZ formatter");
    }
    try {
      Timestamp ts = epochs.isNull(index) ? null : getTimestamp(index, TimeZone.getDefault(), true);

      return ts == null
          ? null
          : context.getTimestampTZFormatter().format(ts, timeZone, context.getScale(columnIndex));
    } catch (TimestampOperationNotAvailableException e) {
      return e.getSecsSinceEpoch().toPlainString();
    }
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    if (epochs.isNull(index)) {
      return null;
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "byteArray", toString(index));
  }

  @Override
  public Object toObject(int index) throws SFException {
    return toTimestamp(index, TimeZone.getDefault());
  }

  @Override
  public Timestamp toTimestamp(int index, TimeZone tz) throws SFException {
    return epochs.isNull(index) ? null : getTimestamp(index, tz, false);
  }

  private Timestamp getTimestamp(int index, TimeZone tz, boolean fromToString) throws SFException {
    long epoch = epochs.getDataBuffer().getLong(index * BigIntVector.TYPE_WIDTH);
    int fraction = fractions.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);
    int timeZoneIndex = timeZoneIndices.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);

    if (ArrowResultUtil.isTimestampOverflow(epoch)) {
      if (fromToString) {
        throw new TimestampOperationNotAvailableException(epoch, fraction);
      } else {
        return null;
      }
    }

    if (context.getResultVersion() > 0) {
      timeZone = SFTimestamp.convertTimezoneIndexToTimeZone(timeZoneIndex);
    } else {
      timeZone = TimeZone.getTimeZone("UTC");
    }
    Timestamp ts = ArrowResultUtil.createTimestamp(epoch, fraction, timeZone, useSessionTimezone);
    Timestamp adjustedTimestamp = ResultUtil.adjustTimestamp(ts);
    return adjustedTimestamp;
  }

  @Override
  public Date toDate(int index, TimeZone tz, boolean dateFormat) throws SFException {
    if (epochs.isNull(index)) {
      return null;
    }
    Timestamp ts = getTimestamp(index, TimeZone.getDefault(), false);
    // ts can be null when Java's timestamp is overflow.
    return ts == null
        ? null
        : new SnowflakeDateWithTimezone(ts.getTime(), timeZone, useSessionTimezone);
  }

  @Override
  public Time toTime(int index) throws SFException {
    Timestamp ts = toTimestamp(index, TimeZone.getDefault());
    return ts == null ? null : new SnowflakeTimeWithTimezone(ts, timeZone, useSessionTimezone);
  }

  @Override
  public boolean toBoolean(int index) throws SFException {
    if (epochs.isNull(index)) {
      return false;
    }
    Timestamp val = toTimestamp(index, TimeZone.getDefault());
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
        SnowflakeUtil.BOOLEAN_STR, val);
  }

  @Override
  public short toShort(int rowIndex) throws SFException {
    if (epochs.isNull(rowIndex)) {
      return 0;
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.SHORT_STR, "");
  }
}
