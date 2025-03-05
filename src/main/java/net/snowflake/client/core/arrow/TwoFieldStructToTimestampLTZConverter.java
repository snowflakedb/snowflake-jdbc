package net.snowflake.client.core.arrow;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeDateWithTimezone;
import net.snowflake.client.jdbc.SnowflakeTimeWithTimezone;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

/** converter from two-field struct (epochs and fraction) to Timestamp_LTZ */
public class TwoFieldStructToTimestampLTZConverter extends AbstractArrowVectorConverter {
  private StructVector structVector;
  private BigIntVector epochs;
  private IntVector fractions;

  /**
   * @param fieldVector ValueVector
   * @param columnIndex column index
   * @param context DataConversionContext
   */
  public TwoFieldStructToTimestampLTZConverter(
      ValueVector fieldVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.TIMESTAMP_LTZ.name(), fieldVector, columnIndex, context);
    structVector = (StructVector) fieldVector;
    epochs = structVector.getChild(FIELD_NAME_EPOCH, BigIntVector.class);
    fractions = structVector.getChild(FIELD_NAME_FRACTION, IntVector.class);
  }

  @Override
  public boolean isNull(int index) {
    return structVector.isNull(index) || epochs.isNull(index) || fractions.isNull(index);
  }

  @Override
  public String toString(int index) throws SFException {
    if (context.getTimestampLTZFormatter() == null) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "missing timestamp LTZ formatter");
    }

    try {
      Timestamp ts = isNull(index) ? null : getTimestamp(index, TimeZone.getDefault(), true);

      return ts == null
          ? null
          : context
              .getTimestampLTZFormatter()
              .format(ts, context.getTimeZone(), context.getScale(columnIndex));
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
    return isNull(index) ? null : getTimestamp(index, tz, false);
  }

  private Timestamp getTimestamp(int index, TimeZone tz, boolean fromToString) throws SFException {
    long epoch = epochs.getDataBuffer().getLong(index * BigIntVector.TYPE_WIDTH);
    int fraction = fractions.getDataBuffer().getInt(index * IntVector.TYPE_WIDTH);

    return getTimestamp(epoch, fraction, sessionTimeZone, useSessionTimezone, fromToString);
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    if (isNull(index)) {
      return null;
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "byteArray", toString(index));
  }

  @Override
  public Date toDate(int index, TimeZone tz, boolean dateFormat) throws SFException {
    if (isNull(index)) {
      return null;
    }
    Timestamp ts = getTimestamp(index, TimeZone.getDefault(), false);
    // ts can be null when Java's timestamp is overflow.
    return ts == null
        ? null
        : new SnowflakeDateWithTimezone(ts.getTime(), sessionTimeZone, useSessionTimezone);
  }

  @Override
  public Time toTime(int index) throws SFException {
    Timestamp ts = toTimestamp(index, TimeZone.getDefault());
    return ts == null
        ? null
        : new SnowflakeTimeWithTimezone(ts, sessionTimeZone, useSessionTimezone);
  }

  @Override
  public boolean toBoolean(int index) throws SFException {
    if (isNull(index)) {
      return false;
    }
    Timestamp val = toTimestamp(index, TimeZone.getDefault());
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
        SnowflakeUtil.BOOLEAN_STR, val);
  }

  public static Timestamp getTimestamp(
      long epoch,
      int fraction,
      TimeZone sessionTimeZone,
      boolean useSessionTimezone,
      boolean fromToString)
      throws SFException {
    if (ArrowResultUtil.isTimestampOverflow(epoch)) {
      if (fromToString) {
        throw new TimestampOperationNotAvailableException(epoch, fraction);
      } else {
        return null;
      }
    }
    Timestamp ts =
        ArrowResultUtil.createTimestamp(epoch, fraction, sessionTimeZone, useSessionTimezone);
    return ResultUtil.adjustTimestamp(ts);
  }
}
