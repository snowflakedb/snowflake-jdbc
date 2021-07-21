/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.Date;
import java.util.TimeZone;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

/**
 * Date with toString() overridden to display date values in session timezone. Only relevant for
 * timestamp objects fetched as dates. Normal date objects do not have a timezone associated with
 * them.
 */
public class SnowflakeDateWithTimezone extends Date {

  TimeZone timezone = TimeZone.getDefault();
  boolean useSessionTimezone = false;
  SnowflakeDateTimeFormat dateFormatter = SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD");

  public SnowflakeDateWithTimezone(
      long date,
      TimeZone timezone,
      boolean useSessionTimezone,
      SnowflakeDateTimeFormat dateFormatter) {
    super(date);
    this.timezone = useSessionTimezone ? timezone : TimeZone.getDefault();
    this.useSessionTimezone = useSessionTimezone;
    this.dateFormatter = dateFormatter;
  }

  public SnowflakeDateWithTimezone(long date, SnowflakeDateTimeFormat dateFormatter) {
    super(date);
    this.dateFormatter = dateFormatter;
  }

  public SnowflakeDateWithTimezone(Date date, SnowflakeDateTimeFormat dateFormatter) {
    super(date.getTime());
    this.dateFormatter = dateFormatter;
  }

  /**
   * Returns a string representation in UTC so as to display "wallclock time"
   *
   * @return a string representation of the object
   */
  public synchronized String toString() {
    return ResultUtil.getDateAsString(this, dateFormatter, timezone);
  }
}
