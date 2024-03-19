/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.TimeZone;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

public class SqlInputTimestampUtil {

  @SnowflakeJdbcInternalApi
  public static Timestamp getTimestampFromType(
      int columnSubType,
      String value,
      SFBaseSession session,
      TimeZone sessionTimeZone,
      TimeZone tz) {
    if (columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ) {
      return getTimestampFromFormat(
          "TIMESTAMP_LTZ_OUTPUT_FORMAT", value, session, sessionTimeZone, tz);
    } else if (columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_NTZ
        || columnSubType == Types.TIMESTAMP) {
      return getTimestampFromFormat(
          "TIMESTAMP_NTZ_OUTPUT_FORMAT", value, session, sessionTimeZone, tz);
    } else if (columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ) {
      return getTimestampFromFormat(
          "TIMESTAMP_TZ_OUTPUT_FORMAT", value, session, sessionTimeZone, tz);
    } else {
      return null;
    }
  }

  private static Timestamp getTimestampFromFormat(
      String format, String value, SFBaseSession session, TimeZone sessionTimeZone, TimeZone tz) {
    String rawFormat = (String) session.getCommonParameters().get(format);
    if (rawFormat == null || rawFormat.isEmpty()) {
      rawFormat = (String) session.getCommonParameters().get("TIMESTAMP_OUTPUT_FORMAT");
    }
    if (tz == null) {
      tz = sessionTimeZone;
    }
    SnowflakeDateTimeFormat formatter = SnowflakeDateTimeFormat.fromSqlFormat(rawFormat);
    return formatter.parse(value, tz, 0, false).getTimestamp();
  }
}
