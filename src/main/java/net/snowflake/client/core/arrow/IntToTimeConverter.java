/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeTimeWithTimezone;
import net.snowflake.client.jdbc.SnowflakeTimestampWithTimezone;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.common.core.SFTime;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

public class IntToTimeConverter extends AbstractArrowVectorConverter {
  private IntVector intVector;
  private ByteBuffer byteBuf = ByteBuffer.allocate(IntVector.TYPE_WIDTH);

  public IntToTimeConverter(
      ValueVector fieldVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.TIME.name(), fieldVector, columnIndex, context);
    this.intVector = (IntVector) fieldVector;
  }

  /**
   * parse long into SFTime
   *
   * @param index
   * @return
   */
  private SFTime toSFTime(int index) {
    long val = intVector.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);
    return SFTime.fromFractionalSeconds(val, context.getScale(columnIndex));
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    if (isNull(index)) {
      return null;
    } else {
      byteBuf.putInt(0, intVector.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH));
      return byteBuf.array();
    }
  }

  @Override
  public Time toTime(int index) throws SFException {
    if (isNull(index)) {
      return null;
    } else {
      SFTime sfTime = toSFTime(index);
      if (sfTime == null) {
        return null;
      }
      return new SnowflakeTimeWithTimezone(
          sfTime.getFractionalSeconds(ResultUtil.DEFAULT_SCALE_OF_SFTIME_FRACTION_SECONDS),
          sfTime.getNanosecondsWithinSecond(),
          useSessionTimezone);
    }
  }

  @Override
  public String toString(int index) throws SFException {
    if (context.getTimeFormatter() == null) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "missing time formatter");
    }
    return isNull(index)
        ? null
        : ResultUtil.getSFTimeAsString(
            toSFTime(index), context.getScale(columnIndex), context.getTimeFormatter());
  }

  @Override
  public Object toObject(int index) throws SFException {
    return isNull(index) ? null : toTime(index);
  }

  @Override
  public Timestamp toTimestamp(int index, TimeZone tz) throws SFException {
    if (isNull(index)) {
      return null;
    }
    if (useSessionTimezone) {
      SFTime sfTime = toSFTime(index);
      return new SnowflakeTimestampWithTimezone(
          sfTime.getFractionalSeconds(ResultUtil.DEFAULT_SCALE_OF_SFTIME_FRACTION_SECONDS),
          sfTime.getNanosecondsWithinSecond(),
          TimeZone.getTimeZone("UTC"));
    }
    return new Timestamp(toTime(index).getTime());
  }

  @Override
  public boolean toBoolean(int index) throws SFException {
    if (isNull(index)) {
      return false;
    }
    Time val = toTime(index);
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Boolean", val);
  }
}
