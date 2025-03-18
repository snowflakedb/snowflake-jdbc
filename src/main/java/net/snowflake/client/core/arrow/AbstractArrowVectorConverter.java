package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.apache.arrow.vector.ValueVector;

/**
 * Abstract class of arrow vector converter. For most types, throw invalid convert error. It depends
 * child class to override conversion logic
 *
 * <p>Note: two method toObject and toString is abstract method because every converter
 * implementation needs to implement them
 */
abstract class AbstractArrowVectorConverter implements ArrowVectorConverter {
  /** snowflake logical type of the target arrow vector */
  protected String logicalTypeStr;

  /** value vector */
  private ValueVector valueVector;

  protected DataConversionContext context;

  protected int columnIndex;

  protected boolean treatNTZasUTC;

  protected boolean useSessionTimezone;

  protected TimeZone sessionTimeZone;

  private boolean shouldTreatDecimalAsInt;

  /** Field names of the struct vectors used by timestamp */
  public static final String FIELD_NAME_EPOCH = "epoch"; // seconds since epoch

  /** Timezone index */
  public static final String FIELD_NAME_TIME_ZONE_INDEX = "timezone"; // time zone index

  /** Fraction in nanoseconds */
  public static final String FIELD_NAME_FRACTION = "fraction"; // fraction in nanoseconds

  /**
   * @param logicalTypeStr snowflake logical type of the target arrow vector.
   * @param valueVector value vector
   * @param vectorIndex value index
   * @param context DataConversionContext
   */
  AbstractArrowVectorConverter(
      String logicalTypeStr,
      ValueVector valueVector,
      int vectorIndex,
      DataConversionContext context) {
    this.logicalTypeStr = logicalTypeStr;
    this.valueVector = valueVector;
    this.columnIndex = vectorIndex + 1;
    this.context = context;
    this.shouldTreatDecimalAsInt =
        context == null
            || context.getSession() == null
            || context.getSession().isJdbcArrowTreatDecimalAsInt()
            || context.getSession().isJdbcTreatDecimalAsInt();
  }

  @Override
  public boolean toBoolean(int rowIndex) throws SFException {
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.BOOLEAN_STR, "");
  }

  @Override
  public byte toByte(int rowIndex) throws SFException {
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.BYTE_STR, "");
  }

  @Override
  public short toShort(int rowIndex) throws SFException {
    if (isNull(rowIndex)) {
      return 0;
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.SHORT_STR, "");
  }

  @Override
  public int toInt(int rowIndex) throws SFException {
    if (isNull(rowIndex)) {
      return 0;
    }
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.INT_STR);
  }

  @Override
  public long toLong(int rowIndex) throws SFException {
    if (isNull(rowIndex)) {
      return 0;
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.LONG_STR, "");
  }

  @Override
  public double toDouble(int rowIndex) throws SFException {
    if (isNull(rowIndex)) {
      return 0;
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.DOUBLE_STR, "");
  }

  @Override
  public float toFloat(int rowIndex) throws SFException {
    if (isNull(rowIndex)) {
      return 0;
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.FLOAT_STR, "");
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    if (isNull(index)) {
      return null;
    }
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "byteArray", "");
  }

  @Override
  public Date toDate(int index, TimeZone jvmTz, boolean useDateFormat) throws SFException {
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.DATE_STR, "");
  }

  @Override
  public Time toTime(int index) throws SFException {
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.TIME_STR, "");
  }

  @Override
  public Timestamp toTimestamp(int index, TimeZone tz) throws SFException {
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.TIMESTAMP_STR, "");
  }

  @Override
  public BigDecimal toBigDecimal(int index) throws SFException {
    if (isNull(index)) {
      return null;
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.BIG_DECIMAL_STR, "");
  }

  /**
   * True if should treat decimal as int type.
   *
   * @return true or false if decimal should be treated as int type.
   */
  boolean shouldTreatDecimalAsInt() {
    return shouldTreatDecimalAsInt;
  }

  @Override
  public void setTreatNTZAsUTC(boolean isUTC) {
    this.treatNTZasUTC = isUTC;
  }

  @Override
  public void setUseSessionTimezone(boolean useSessionTimezone) {
    this.useSessionTimezone = useSessionTimezone;
  }

  @Override
  public void setSessionTimeZone(TimeZone tz) {
    this.sessionTimeZone = tz;
  }

  @Override
  public boolean isNull(int index) {
    return valueVector.isNull(index);
  }

  @Override
  public abstract Object toObject(int index) throws SFException;

  @Override
  public abstract String toString(int index) throws SFException;

  /**
   * Thrown when a Snowflake timestamp cannot be manipulated in Java due to size limitations.
   * Snowflake can use up to a full SB16 to represent a timestamp. Java, on the other hand, requires
   * that the number of millis since epoch fit into a long. For timestamps whose millis since epoch
   * don't fit into a long, certain operations, such as conversion to java .sql.Timestamp, are not
   * available.
   */
  public static class TimestampOperationNotAvailableException extends RuntimeException {
    private BigDecimal secsSinceEpoch;

    TimestampOperationNotAvailableException(long secsSinceEpoch, int fraction) {
      super("seconds=" + secsSinceEpoch + " nanos=" + fraction);
      this.secsSinceEpoch =
          new BigDecimal(secsSinceEpoch).add(new BigDecimal(fraction).scaleByPowerOfTen(-9));
    }

    public BigDecimal getSecsSinceEpoch() {
      return secsSinceEpoch;
    }
  }
}
