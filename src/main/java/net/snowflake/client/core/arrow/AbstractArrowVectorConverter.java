/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.common.core.SqlState;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;

/**
 * Abstract class of arrow vector converter. For most types, throw invalid convert error. It depends
 * child class to override conversion logic
 *
 * <p>Note: two method toObject and toString is abstract method because every converter
 * implementation needs to implement them
 */
@SnowflakeJdbcInternalApi
public abstract class AbstractArrowVectorConverter implements ArrowVectorConverter {
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

  public static final String FIELD_NAME_TIME_ZONE_INDEX = "timezone"; // time zone index
  public static final String FIELD_NAME_FRACTION = "fraction"; // fraction in nanoseconds

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

  /**
   * Given an arrow vector (a single column in a single record batch), return an arrow vector
   * converter. Note, converter is built on top of arrow vector, so that arrow data can be converted
   * back to java data
   *
   * <p>
   *
   * <p>Arrow converter mappings for Snowflake fixed-point numbers
   * ----------------------------------------------------------------------------------------- Max
   * position and scale Converter
   * -----------------------------------------------------------------------------------------
   * number(3,0) {@link TinyIntToFixedConverter} number(3,2) {@link TinyIntToScaledFixedConverter}
   * number(5,0) {@link SmallIntToFixedConverter} number(5,4) {@link SmallIntToScaledFixedConverter}
   * number(10,0) {@link IntToFixedConverter} number(10,9) {@link IntToScaledFixedConverter}
   * number(19,0) {@link BigIntToFixedConverter} number(19,18) {@link BigIntToFixedConverter}
   * number(38,37) {@link DecimalToScaledFixedConverter}
   * ------------------------------------------------------------------------------------------
   *
   * @param vector an arrow vector
   * @param context data conversion context
   * @param session SFBaseSession for purposes of logging
   * @param idx the index of the vector in its batch
   * @return A converter on top og the vector
   */
  public static ArrowVectorConverter initConverter(
      ValueVector vector, DataConversionContext context, SFBaseSession session, int idx)
      throws SnowflakeSQLException {
    // arrow minor type
    Types.MinorType type = Types.getMinorTypeForArrowType(vector.getField().getType());

    // each column's metadata
    Map<String, String> customMeta = vector.getField().getMetadata();
    if (type == Types.MinorType.DECIMAL) {
      // Note: Decimal vector is different from others
      return new DecimalToScaledFixedConverter(vector, idx, context);
    } else if (!customMeta.isEmpty()) {
      SnowflakeType st = SnowflakeType.valueOf(customMeta.get("logicalType"));
      switch (st) {
        case ANY:
        case CHAR:
        case TEXT:
        case VARIANT:
          return new VarCharConverter(vector, idx, context);

        case MAP:
          if (vector instanceof MapVector) {
            return new MapConverter((MapVector) vector, idx, context);
          } else {
            return new VarCharConverter(vector, idx, context);
          }

        case VECTOR:
          return new VectorTypeConverter((FixedSizeListVector) vector, idx, context);

        case ARRAY:
          if (vector instanceof ListVector) {
            return new ArrayConverter((ListVector) vector, idx, context);
          } else {
            return new VarCharConverter(vector, idx, context);
          }

        case OBJECT:
          if (vector instanceof StructVector) {
            return new StructConverter((StructVector) vector, idx, context);
          } else {
            return new VarCharConverter(vector, idx, context);
          }

        case BINARY:
          return new VarBinaryToBinaryConverter(vector, idx, context);

        case BOOLEAN:
          return new BitToBooleanConverter(vector, idx, context);

        case DATE:
          boolean getFormatDateWithTimeZone = false;
          if (context.getSession() != null) {
            getFormatDateWithTimeZone = context.getSession().getFormatDateWithTimezone();
          }
          return new DateConverter(vector, idx, context, getFormatDateWithTimeZone);

        case FIXED:
          String scaleStr = vector.getField().getMetadata().get("scale");
          int sfScale = Integer.parseInt(scaleStr);
          switch (type) {
            case TINYINT:
              if (sfScale == 0) {
                return new TinyIntToFixedConverter(vector, idx, context);
              } else {
                return new TinyIntToScaledFixedConverter(vector, idx, context, sfScale);
              }
            case SMALLINT:
              if (sfScale == 0) {
                return new SmallIntToFixedConverter(vector, idx, context);
              } else {
                return new SmallIntToScaledFixedConverter(vector, idx, context, sfScale);
              }
            case INT:
              if (sfScale == 0) {
                return new IntToFixedConverter(vector, idx, context);
              } else {
                return new IntToScaledFixedConverter(vector, idx, context, sfScale);
              }
            case BIGINT:
              if (sfScale == 0) {
                return new BigIntToFixedConverter(vector, idx, context);
              } else {
                return new BigIntToScaledFixedConverter(vector, idx, context, sfScale);
              }
          }
          break;

        case REAL:
          return new DoubleToRealConverter(vector, idx, context);

        case TIME:
          switch (type) {
            case INT:
              return new IntToTimeConverter(vector, idx, context);
            case BIGINT:
              return new BigIntToTimeConverter(vector, idx, context);
            default:
              throw new SnowflakeSQLLoggedException(
                  session,
                  ErrorCode.INTERNAL_ERROR.getMessageCode(),
                  SqlState.INTERNAL_ERROR,
                  "Unexpected Arrow Field for ",
                  st.name());
          }

        case TIMESTAMP_LTZ:
          if (vector.getField().getChildren().isEmpty()) {
            // case when the scale of the timestamp is equal or smaller than millisecs since epoch
            return new BigIntToTimestampLTZConverter(vector, idx, context);
          } else if (vector.getField().getChildren().size() == 2) {
            // case when the scale of the timestamp is larger than millisecs since epoch, e.g.,
            // nanosecs
            return new TwoFieldStructToTimestampLTZConverter(vector, idx, context);
          } else {
            throw new SnowflakeSQLLoggedException(
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "Unexpected Arrow Field for ",
                st.name());
          }

        case TIMESTAMP_NTZ:
          if (vector.getField().getChildren().isEmpty()) {
            // case when the scale of the timestamp is equal or smaller than 7
            return new BigIntToTimestampNTZConverter(vector, idx, context);
          } else if (vector.getField().getChildren().size() == 2) {
            // when the timestamp is represent in two-field struct
            return new TwoFieldStructToTimestampNTZConverter(vector, idx, context);
          } else {
            throw new SnowflakeSQLLoggedException(
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "Unexpected Arrow Field for ",
                st.name());
          }

        case TIMESTAMP_TZ:
          if (vector.getField().getChildren().size() == 2) {
            // case when the scale of the timestamp is equal or smaller than millisecs since epoch
            return new TwoFieldStructToTimestampTZConverter(vector, idx, context);
          } else if (vector.getField().getChildren().size() == 3) {
            // case when the scale of the timestamp is larger than millisecs since epoch, e.g.,
            // nanosecs
            return new ThreeFieldStructToTimestampTZConverter(vector, idx, context);
          } else {
            throw new SnowflakeSQLLoggedException(
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "Unexpected SnowflakeType ",
                st.name());
          }

        default:
          throw new SnowflakeSQLLoggedException(
              session,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              SqlState.INTERNAL_ERROR,
              "Unexpected Arrow Field for ",
              st.name());
      }
    }
    throw new SnowflakeSQLLoggedException(
        session,
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        SqlState.INTERNAL_ERROR,
        "Unexpected Arrow Field for ",
        type.toString());
  }
}
