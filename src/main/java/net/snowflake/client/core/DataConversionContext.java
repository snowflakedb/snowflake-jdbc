/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

/**
 * This class contains formatter info about each data type and related flags
 * etc. And it is scoped to a single result set. a.k.a each result set object
 * should have its own formatter info
 */
public class DataConversionContext
{
  /**
   * timestamp_ltz formatter
   */
  final private SnowflakeDateTimeFormat timestampLTZFormatter;

  /**
   * timestamp_ntz formatter
   */
  final private SnowflakeDateTimeFormat timestampNTZFormatter;

  /**
   * timestamp_tz formatter
   */
  final private SnowflakeDateTimeFormat timestampTZFormatter;

  /**
   * date formatter
   */
  final private SnowflakeDateTimeFormat dateFormatter;

  /**
   * time formatter
   */
  final private SnowflakeDateTimeFormat timeFormatter;

  /**
   * binary formatter
   */
  private SFBinaryFormat binaryFormatter;

  public DataConversionContext(
      SnowflakeDateTimeFormat timestampLTZFormatter,
      SnowflakeDateTimeFormat timestampNTZFormatter,
      SnowflakeDateTimeFormat timestampTZFormatter,
      SnowflakeDateTimeFormat dateFormatter,
      SnowflakeDateTimeFormat timeFormatter,
      SFBinaryFormat binaryFormatter)
  {
    this.timestampLTZFormatter = timestampLTZFormatter;
    this.timestampNTZFormatter = timestampNTZFormatter;
    this.timestampTZFormatter = timestampTZFormatter;
    this.dateFormatter = dateFormatter;
    this.timeFormatter = timeFormatter;
    this.binaryFormatter = binaryFormatter;
  }

  public SnowflakeDateTimeFormat getTimestampLTZFormatter()
  {
    return timestampLTZFormatter;
  }

  public SnowflakeDateTimeFormat getTimestampNTZFormatter()
  {
    return timestampNTZFormatter;
  }

  public SnowflakeDateTimeFormat getTimestampTZFormatter()
  {
    return timestampTZFormatter;
  }

  public SnowflakeDateTimeFormat getDateFormatter()
  {
    return dateFormatter;
  }

  public SnowflakeDateTimeFormat getTimeFormatter()
  {
    return timeFormatter;
  }

  public SFBinaryFormat getBinaryFormatter()
  {
    return binaryFormatter;
  }
}
