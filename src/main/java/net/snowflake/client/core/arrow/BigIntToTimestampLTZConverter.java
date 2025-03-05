package net.snowflake.client.core.arrow;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;

/** converter from BigInt (Long) to Timestamp_LTZ */
public class BigIntToTimestampLTZConverter extends AbstractArrowVectorConverter {
  private BigIntVector bigIntVector;
  private ByteBuffer byteBuf = ByteBuffer.allocate(BigIntVector.TYPE_WIDTH);

  /**
   * @param fieldVector ValueVector
   * @param columnIndex column index
   * @param context DataConversionContext
   */
  public BigIntToTimestampLTZConverter(
      ValueVector fieldVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.TIMESTAMP_LTZ.name(), fieldVector, columnIndex, context);
    this.bigIntVector = (BigIntVector) fieldVector;
  }

  @Override
  public String toString(int index) throws SFException {
    if (context.getTimestampLTZFormatter() == null) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "missing timestamp LTZ formatter");
    }
    Timestamp ts = toTimestamp(index, TimeZone.getDefault());

    return ts == null
        ? null
        : context
            .getTimestampLTZFormatter()
            .format(ts, context.getTimeZone(), context.getScale(columnIndex));
  }

  @Override
  public byte[] toBytes(int index) {
    if (isNull(index)) {
      return null;
    } else {
      byteBuf.putLong(0, bigIntVector.getDataBuffer().getLong(index * BigIntVector.TYPE_WIDTH));
      return byteBuf.array();
    }
  }

  @Override
  public Object toObject(int index) throws SFException {
    return toTimestamp(index, TimeZone.getDefault());
  }

  @Override
  public Timestamp toTimestamp(int index, TimeZone tz) throws SFException {
    return isNull(index) ? null : getTimestamp(index, tz);
  }

  private Timestamp getTimestamp(int index, TimeZone tz) throws SFException {
    long val = bigIntVector.getDataBuffer().getLong(index * BigIntVector.TYPE_WIDTH);
    int scale = context.getScale(columnIndex);
    return getTimestamp(val, scale, sessionTimeZone, useSessionTimezone);
  }

  @Override
  public Date toDate(int index, TimeZone tz, boolean useDateFormat) throws SFException {
    return isNull(index) ? null : new Date(getTimestamp(index, TimeZone.getDefault()).getTime());
  }

  @Override
  public Time toTime(int index) throws SFException {
    Timestamp ts = toTimestamp(index, TimeZone.getDefault());
    return ts == null ? null : new Time(ts.getTime());
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

  /**
   * Use {@link #getTimestamp(long, int, TimeZone, boolean)}
   *
   * @param val epoch
   * @param scale scale
   * @return Timestamp value without timezone take into account
   * @throws SFException if exception encountered
   */
  @Deprecated
  public static Timestamp getTimestamp(long val, int scale) throws SFException {
    Timestamp ts = ArrowResultUtil.toJavaTimestamp(val, scale);
    return ResultUtil.adjustTimestamp(ts);
  }

  @SnowflakeJdbcInternalApi
  public static Timestamp getTimestamp(
      long epoch, int scale, TimeZone sessionTimeZone, boolean useSessionTimezone)
      throws SFException {
    return ResultUtil.adjustTimestamp(
        ArrowResultUtil.toJavaTimestamp(epoch, scale, sessionTimeZone, useSessionTimezone));
  }
}
