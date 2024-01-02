package net.snowflake.client.core.json;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.TimeZone;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.ArrowResultUtil;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeDateWithTimezone;
import net.snowflake.client.jdbc.SnowflakeTimeWithTimezone;
import net.snowflake.client.jdbc.SnowflakeTimestampWithTimezone;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.common.core.SFTime;
import net.snowflake.common.core.SFTimestamp;

public class DateTimeConverter {
  private final TimeZone sessionTimeZone;
  private final long resultVersion;
  private final boolean honorClientTZForTimestampNTZ;
  private final boolean treatNTZAsUTC;
  private final boolean useSessionTimezone;
  private final boolean formatDateWithTimeZone;
  private final SFBaseSession session;

  public DateTimeConverter(
      TimeZone sessionTimeZone,
      SFBaseSession session,
      long resultVersion,
      boolean honorClientTZForTimestampNTZ,
      boolean treatNTZAsUTC,
      boolean useSessionTimezone,
      boolean formatDateWithTimeZone) {
    this.sessionTimeZone = sessionTimeZone;
    this.session = session;
    this.resultVersion = resultVersion;
    this.honorClientTZForTimestampNTZ = honorClientTZForTimestampNTZ;
    this.treatNTZAsUTC = treatNTZAsUTC;
    this.useSessionTimezone = useSessionTimezone;
    this.formatDateWithTimeZone = formatDateWithTimeZone;
  }

  public Timestamp getTimestamp(
      Object obj, int columnType, int columnSubType, TimeZone tz, int scale) throws SFException {
    if (obj == null) {
      return null;
    }
    if (Types.TIMESTAMP == columnType || Types.TIMESTAMP_WITH_TIMEZONE == columnType) {
      if (tz == null) {
        tz = TimeZone.getDefault();
      }
      SFTimestamp sfTS =
          ResultUtil.getSFTimestamp(
              obj.toString(), scale, columnSubType, resultVersion, sessionTimeZone, session);

      Timestamp res = sfTS.getTimestamp();
      if (res == null) {
        return null;
      }
      // If we want to display format with no session offset, we have to use session timezone for
      // ltz and tz types but UTC timezone for ntz type.
      if (useSessionTimezone) {
        if (columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ
            || columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ) {
          TimeZone specificSessionTimezone = adjustTimezoneForTimestampTZ(obj, columnSubType);
          res = new SnowflakeTimestampWithTimezone(res, specificSessionTimezone);
        } else {
          res = new SnowflakeTimestampWithTimezone(res);
        }
      }
      // If timestamp type is NTZ and JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC=true, keep
      // timezone in UTC to avoid daylight savings errors
      else if (treatNTZAsUTC && columnSubType == Types.TIMESTAMP) {
        res = new SnowflakeTimestampWithTimezone(res);
      }
      // If JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC=false, default behavior is to honor
      // client timezone for NTZ time. Move NTZ timestamp offset to correspond to
      // client's timezone. JDBC_USE_SESSION_TIMEZONE overrides other params.
      if (columnSubType == Types.TIMESTAMP
          && ((!treatNTZAsUTC && honorClientTZForTimestampNTZ) || useSessionTimezone)) {
        res = sfTS.moveToTimeZone(tz).getTimestamp();
      }
      // Adjust time if date happens before year 1582 for difference between
      // Julian and Gregorian calendars
      return ResultUtil.adjustTimestamp(res);
    } else if (Types.DATE == columnType) {
      Date d = getDate(obj, columnType, columnSubType, tz, scale);
      if (d == null) {
        return null;
      }
      return new Timestamp(d.getTime());
    } else if (Types.TIME == columnType) {
      Time t = getTime(obj, columnType, columnSubType, tz, scale);
      if (t == null) {
        return null;
      }
      if (useSessionTimezone) {
        SFTime sfTime = ResultUtil.getSFTime(obj.toString(), scale, session);
        return new SnowflakeTimestampWithTimezone(
            sfTime.getFractionalSeconds(ResultUtil.DEFAULT_SCALE_OF_SFTIME_FRACTION_SECONDS),
            sfTime.getNanosecondsWithinSecond(),
            TimeZone.getTimeZone("UTC"));
      }
      return new Timestamp(t.getTime());
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.TIMESTAMP_STR, obj);
    }
  }

  public Time getTime(Object obj, int columnType, int columnSubType, TimeZone tz, int scale)
      throws SFException {
    if (obj == null) {
      return null;
    }
    if (Types.TIME == columnType) {
      SFTime sfTime = ResultUtil.getSFTime(obj.toString(), scale, session);
      Time ts =
          new Time(
              sfTime.getFractionalSeconds(ResultUtil.DEFAULT_SCALE_OF_SFTIME_FRACTION_SECONDS));
      if (useSessionTimezone) {
        ts =
            SnowflakeUtil.getTimeInSessionTimezone(
                SnowflakeUtil.getSecondsFromMillis(ts.getTime()),
                sfTime.getNanosecondsWithinSecond());
      }
      return ts;
    } else if (Types.TIMESTAMP == columnType || Types.TIMESTAMP_WITH_TIMEZONE == columnType) {
      Timestamp ts = getTimestamp(obj, columnType, columnSubType, tz, scale);
      if (ts == null) {
        return null;
      }
      if (useSessionTimezone) {
        ts = getTimestamp(obj, columnType, columnSubType, sessionTimeZone, scale);
        TimeZone sessionTimeZone = adjustTimezoneForTimestampTZ(obj, columnSubType);
        return new SnowflakeTimeWithTimezone(ts, sessionTimeZone, useSessionTimezone);
      }
      return new Time(ts.getTime());
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.TIME_STR, obj);
    }
  }

  public Date getDate(Object obj, int columnType, int columnSubType, TimeZone tz, int scale)
      throws SFException {
    if (obj == null) {
      return null;
    }

    if (Types.TIMESTAMP == columnType || Types.TIMESTAMP_WITH_TIMEZONE == columnType) {
      if (tz == null) {
        tz = TimeZone.getDefault();
      }
      if (columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ
          || columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ) {
        TimeZone specificSessionTimeZone = adjustTimezoneForTimestampTZ(obj, columnSubType);
        return new SnowflakeDateWithTimezone(
            getTimestamp(obj, columnType, columnSubType, tz, scale).getTime(),
            specificSessionTimeZone,
            useSessionTimezone);
      }
      return new Date(getTimestamp(obj, columnType, columnSubType, tz, scale).getTime());

    } else if (Types.DATE == columnType) {
      if (tz == null || !formatDateWithTimeZone) {
        return ArrowResultUtil.getDate(Integer.parseInt((String) obj));
      }
      return ArrowResultUtil.getDate(Integer.parseInt((String) obj), tz, sessionTimeZone);
    }
    // for Types.TIME and all other type, throw user error
    else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.DATE_STR, obj);
    }
  }

  private TimeZone adjustTimezoneForTimestampTZ(Object obj, int columnSubType) {
    // If the timestamp is of type timestamp_tz, use the associated offset timezone instead of the
    // session timezone for formatting
    if (obj != null
        && columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ
        && resultVersion > 0) {
      String timestampStr = obj.toString();
      int indexForSeparator = timestampStr.indexOf(' ');
      String timezoneIndexStr = timestampStr.substring(indexForSeparator + 1);
      return SFTimestamp.convertTimezoneIndexToTimeZone(Integer.parseInt(timezoneIndexStr));
    }
    // By default, return session timezone
    return sessionTimeZone;
  }
}
