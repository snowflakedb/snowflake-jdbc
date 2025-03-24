package net.snowflake.client.core;

import java.util.TimeZone;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

/**
 * This class contains formatter info about each data type and related flags etc. And it is scoped
 * to a single result set. a.k.a each result set object should have its own formatter info
 */
public interface DataConversionContext {
  /**
   * @return timestamp_ltz formatter
   */
  SnowflakeDateTimeFormat getTimestampLTZFormatter();

  /**
   * @return timestamp_ntz formatter
   */
  SnowflakeDateTimeFormat getTimestampNTZFormatter();

  /**
   * @return timestamp_ntz formatter
   */
  SnowflakeDateTimeFormat getTimestampTZFormatter();

  /**
   * @return date formatter
   */
  SnowflakeDateTimeFormat getDateFormatter();

  /**
   * @return time formatter
   */
  SnowflakeDateTimeFormat getTimeFormatter();

  /**
   * @return binary formatter
   */
  SFBinaryFormat getBinaryFormatter();

  /**
   * get scale from Snowflake metadata
   *
   * @param columnIndex column index
   * @return scale value
   */
  int getScale(int columnIndex);

  /**
   * @return current session
   */
  SFBaseSession getSession();

  /**
   * @return session time zone
   */
  TimeZone getTimeZone();

  /**
   * @return whether to honor client time zone for timestamp_ntz
   */
  boolean getHonorClientTZForTimestampNTZ();

  /**
   * @return result version
   */
  long getResultVersion();
}
