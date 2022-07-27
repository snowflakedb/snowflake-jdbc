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
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeTimeWithTimezone;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

/** converter from two-field struct (epochs and fraction) to Timestamp_NTZ */
public class TwoFieldStructToTimestampNTZConverter extends AbstractArrowVectorConverter {
  private StructVector structVector;
  private BigIntVector epochs;
  private IntVector fractions;

  private static final TimeZone NTZ = TimeZone.getTimeZone("UTC");

  public TwoFieldStructToTimestampNTZConverter(
      ValueVector fieldVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.TIMESTAMP_NTZ.name(), fieldVector, columnIndex, context);
    structVector = (StructVector) fieldVector;
    epochs = structVector.getChild(FIELD_NAME_EPOCH, BigIntVector.class);
    fractions = structVector.getChild(FIELD_NAME_FRACTION, IntVector.class);
  }

  @Override
  public boolean isNull(int index) {
    return epochs.isNull(index);
  }

  @Override
  public String toString(int index) throws SFException {
    if (context.getTimestampNTZFormatter() == null) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "missing timestamp NTZ formatter");
    }
    try {
      Timestamp ts = epochs.isNull(index) ? null : getTimestamp(index, TimeZone.getDefault(), true);

      return ts == null
          ? null
          : context
              .getTimestampNTZFormatter()
              .format(ts, TimeZone.getTimeZone("UTC"), context.getScale(columnIndex));
    } catch (TimestampOperationNotAvailableException e) {
      return e.getSecsSinceEpoch().toPlainString();
    }
  }

  @Override
  public Object toObject(int index) throws SFException {
    return toTimestamp(index, TimeZone.getDefault());
  }

  @Override
  public Timestamp toTimestamp(int index, TimeZone tz) throws SFException {
    if (tz == null) {
      tz = TimeZone.getDefault();
    }
    return isNull(index) ? null : getTimestamp(index, tz, false);
  }

  private Timestamp getTimestamp(int index, TimeZone tz, boolean fromToString) throws SFException {
    long epoch = epochs.getDataBuffer().getLong(index * BigIntVector.TYPE_WIDTH);
    int fraction = fractions.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);

    if (ArrowResultUtil.isTimestampOverflow(epoch)) {
      if (fromToString) {
        throw new TimestampOperationNotAvailableException(epoch, fraction);
      } else {
        return null;
      }
    }
    Timestamp ts;
    if (this.treatNTZasUTC || !this.useSessionTimezone) {
      ts = ArrowResultUtil.createTimestamp(epoch, fraction, TimeZone.getTimeZone("UTC"), true);
    } else {
      ts = ArrowResultUtil.createTimestamp(epoch, fraction, sessionTimeZone, false);
    }

    // Note: honorClientTZForTimestampNTZ is not enabled for toString method.
    // If JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC=false, default behavior is to honor
    // client timezone for NTZ time. Move NTZ timestamp offset to correspond to
    // client's timezone. UseSessionTimezone overrides treatNTZasUTC.
    if (!fromToString
        && ((context.getHonorClientTZForTimestampNTZ() && !this.treatNTZasUTC)
            || this.useSessionTimezone)) {
      ts = ArrowResultUtil.moveToTimeZone(ts, NTZ, tz);
    }
    Timestamp adjustedTimestamp = ResultUtil.adjustTimestamp(ts);
    return adjustedTimestamp;
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
  public Date toDate(int index, TimeZone tz, boolean dateFormat) throws SFException {
    return isNull(index)
        ? null
        : new Date(getTimestamp(index, TimeZone.getDefault(), false).getTime());
  }

  @Override
  public Time toTime(int index) throws SFException {
    Timestamp ts = toTimestamp(index, null);
    if (useSessionTimezone) {
      ts = toTimestamp(index, sessionTimeZone);
    }
    return ts == null
        ? null
        : new SnowflakeTimeWithTimezone(ts, sessionTimeZone, useSessionTimezone);
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
}
