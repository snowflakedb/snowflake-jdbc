/*
 * Copyright (c) 2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.util;

import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.Date;

public class SFTimestamp
{
  /**
   * Get current time in UTC in the following format
   *
   * @return String representation in this format: yyyy-MM-dd HH:mm:ss
   */
  public static String getUTCNow()
  {
    SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT"));

    //Time in GMT
    return dateFormatGmt.format(new Date());
  }

}
