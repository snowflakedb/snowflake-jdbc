/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
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
   * timestamp_ltz formatter
   *
   * @return SnowflakeDateTimeFormat
   */
  SnowflakeDateTimeFormat getTimestampLTZFormatter();

  /**
   * timestamp_ntz formatter
   *
   * @return SnowflakeDateTimeFormat
   */
  SnowflakeDateTimeFormat getTimestampNTZFormatter();

  /**
   * timestamp_tz formatter
   *
   * @return SnowflakeDateTimeFormat
   */
  SnowflakeDateTimeFormat getTimestampTZFormatter();

  /**
   * date formatter
   *
   * @return SnowflakeDateTimeFormat
   */
  SnowflakeDateTimeFormat getDateFormatter();

  /**
   * time formatter
   *
   * @return SnowflakeDateTimeFormat
   */
  SnowflakeDateTimeFormat getTimeFormatter();

  /**
   * binary formatter
   *
   * @return SFBinaryFormat
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
