package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import net.snowflake.client.core.SFException;

/** Interface to convert from arrow vector values into java data types. */
public interface ArrowVectorConverter {

  /**
   * Set to true when time value should be displayed in wallclock time (no timezone offset)
   *
   * @param useSessionTimezone boolean value indicating if there is a timezone offset.
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
   * Set to true if NTZ timestamp should be set to UTC
   *
   * @param isUTC true or false value of whether NTZ timestamp should be set to UTC
   */
  void setTreatNTZAsUTC(boolean isUTC);
}
