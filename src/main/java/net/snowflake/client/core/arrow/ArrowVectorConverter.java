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
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.common.core.SqlState;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;

/** Interface to convert from arrow vector values into java data types. */
public interface ArrowVectorConverter {

  /**
   * Set to true when time value should be displayed in wallclock time (no timezone offset)
   *
   * @param useSessionTimezone
   */
  void setUseSessionTimezone(boolean useSessionTimezone);

  void setSessionTimeZone(TimeZone tz);

  /**
   * Determine whether source value in arrow vector is null value or not
   *
   * @param index index of value to be checked
   * @return true if null value otherwise false
   */
  boolean isNull(int index);

  /**
   * Convert value in arrow vector to boolean data
   *
   * @param index index of the value to be converted in the vector
   * @return boolean data converted from arrow vector
   * @throws SFException invalid data conversion
   */
  boolean toBoolean(int index) throws SFException;

  /**
   * Convert value in arrow vector to byte data
   *
   * @param index index of the value to be converted in the vector
   * @return byte data converted from arrow vector
   * @throws SFException invalid data conversion
   */
  byte toByte(int index) throws SFException;

  /**
   * Convert value in arrow vector to short data
   *
   * @param index index of the value to be converted in the vector
   * @return short data converted from arrow vector
   * @throws SFException invalid data conversion
   */
  short toShort(int index) throws SFException;

  /**
   * Convert value in arrow vector to int data
   *
   * @param index index of the value to be converted in the vector
   * @return int data converted from arrow vector
   * @throws SFException invalid data conversion
   */
  int toInt(int index) throws SFException;

  /**
   * Convert value in arrow vector to long data
   *
   * @param index index of the value to be converted in the vector
   * @return long data converted from arrow vector
   * @throws SFException invalid data conversion
   */
  long toLong(int index) throws SFException;

  /**
   * Convert value in arrow vector to double data
   *
   * @param index index of the value to be converted in the vector
   * @return double data converted from arrow vector
   * @throws SFException invalid data conversion
   */
  double toDouble(int index) throws SFException;

  /**
   * Convert value in arrow vector to float data
   *
   * @param index index of the value to be converted in the vector
   * @return float data converted from arrow vector
   * @throws SFException invalid data conversion
   */
  float toFloat(int index) throws SFException;

  /**
   * Convert value in arrow vector to byte array
   *
   * @param index index of the value to be converted in the vector
   * @return byte array converted from arrow vector
   * @throws SFException invalid data conversion
   */
  byte[] toBytes(int index) throws SFException;

  /**
   * Convert value in arrow vector to string
   *
   * @param index index of the value to be converted in the vector
   * @return string converted from arrow vector
   * @throws SFException invalid data conversion
   */
  String toString(int index) throws SFException;

  /**
   * Convert value in arrow vector to Date
   *
   * @param index index of the value to be converted in the vector
   * @param jvmTz JVM timezone
   * @param useDateFormat boolean value to check whether to change timezone or not
   * @return Date converted from arrow vector
   * @throws SFException invalid data conversion
   */
  Date toDate(int index, TimeZone jvmTz, boolean useDateFormat) throws SFException;

  /**
   * Convert value in arrow vector to Time
   *
   * @param index index of the value to be converted in the vector
   * @return Time converted from arrow vector
   * @throws SFException invalid data conversion
   */
  Time toTime(int index) throws SFException;

  /**
   * Convert value in arrow vector to Timestamp
   *
   * @param index index of the value to be converted in the vector
   * @param tz time zone
   * @return Timestamp converted from arrow vector
   * @throws SFException invalid data conversion
   */
  Timestamp toTimestamp(int index, TimeZone tz) throws SFException;

  /**
   * Convert value in arrow vector to BigDecimal
   *
   * @param index index of the value to be converted in the vector
   * @return BigDecimal converted from arrow vector
   * @throws SFException invalid data conversion
   */
  BigDecimal toBigDecimal(int index) throws SFException;

  /**
   * Convert value in arrow vector to Object
   *
   * @param index index of the value to be converted in the vector
   * @return Object converted from arrow vector
   * @throws SFException invalid data conversion
   */
  Object toObject(int index) throws SFException;

  /**
   * @param isUTC true or false value of whether NTZ timestamp should be set to UTC
   */
  void setTreatNTZAsUTC(boolean isUTC);

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
  static ArrowVectorConverter initConverter(
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
