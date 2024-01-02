package net.snowflake.client.core.json;

import java.sql.Date;
import java.sql.Types;
import java.util.TimeZone;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SFTime;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

public class StringConverter {
  private static final SFLogger logger = SFLoggerFactory.getLogger(StringConverter.class);
  private final TimeZone sessionTimeZone;
  private final SFBinaryFormat binaryFormatter;
  private final SnowflakeDateTimeFormat dateFormatter;
  private final SnowflakeDateTimeFormat timeFormatter;
  private final SnowflakeDateTimeFormat timestampNTZFormatter;
  private final SnowflakeDateTimeFormat timestampLTZFormatter;
  private final SnowflakeDateTimeFormat timestampTZFormatter;
  private final long resultVersion;
  private final SFBaseSession session;
  private final Converters converters;

  public StringConverter(
      TimeZone sessionTimeZone,
      SFBinaryFormat binaryFormatter,
      SnowflakeDateTimeFormat dateFormatter,
      SnowflakeDateTimeFormat timeFormatter,
      SnowflakeDateTimeFormat timestampNTZFormatter,
      SnowflakeDateTimeFormat timestampLTZFormatter,
      SnowflakeDateTimeFormat timestampTZFormatter,
      long resultVersion,
      SFBaseSession session,
      Converters converters) {
    this.sessionTimeZone = sessionTimeZone;
    this.binaryFormatter = binaryFormatter;
    this.dateFormatter = dateFormatter;
    this.timeFormatter = timeFormatter;
    this.timestampNTZFormatter = timestampNTZFormatter;
    this.timestampLTZFormatter = timestampLTZFormatter;
    this.timestampTZFormatter = timestampTZFormatter;
    this.resultVersion = resultVersion;
    this.session = session;
    this.converters = converters;
  }

  public String getString(Object obj, int columnType, int columnSubType, int scale)
      throws SFException {
    if (obj == null) {
      return null;
    }

    switch (columnType) {
      case Types.BOOLEAN:
        return ResultUtil.getBooleanAsString(ResultUtil.getBoolean(obj.toString()));

      case Types.TIMESTAMP:
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ:
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ:
        return timestampToString(obj, columnType, columnSubType, scale);
      case Types.DATE:
        return dateToString(obj, columnType, columnSubType, scale);
      case Types.TIME:
        return timeToString(obj, scale);
      case Types.BINARY:
        return binaryToString(obj, columnType, columnSubType, scale);
      default:
        break;
    }
    return obj.toString();
  }

  private String timestampToString(Object obj, int columnType, int columnSubType, int scale)
      throws SFException {
    SFTimestamp sfTS =
        ResultUtil.getSFTimestamp(
            obj.toString(), scale, columnSubType, resultVersion, sessionTimeZone, session);

    String timestampStr =
        ResultUtil.getSFTimestampAsString(
            sfTS,
            columnType,
            scale,
            timestampNTZFormatter,
            timestampLTZFormatter,
            timestampTZFormatter,
            session);

    logger.debug(
        "Converting timestamp to string from: {} to: {}",
        (ArgSupplier) obj::toString,
        timestampStr);

    return timestampStr;
  }

  private String dateToString(Object obj, int columnType, int columnSubType, int scale)
      throws SFException {
    Date date =
        converters
            .getDateTimeConverter()
            .getDate(obj, columnType, columnSubType, TimeZone.getDefault(), scale);

    if (dateFormatter == null) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "missing date formatter");
    }

    String dateStr = ResultUtil.getDateAsString(date, dateFormatter);

    logger.debug("Converting date to string from: {} to: {}", (ArgSupplier) obj::toString, dateStr);

    return dateStr;
  }

  private String timeToString(Object obj, int scale) throws SFException {
    SFTime sfTime = ResultUtil.getSFTime(obj.toString(), scale, session);

    if (timeFormatter == null) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "missing time formatter");
    }

    String timeStr = ResultUtil.getSFTimeAsString(sfTime, scale, timeFormatter);

    logger.debug("Converting time to string from: {} to: {}", (ArgSupplier) obj::toString, timeStr);

    return timeStr;
  }

  private String binaryToString(Object obj, int columnType, int columnSubType, int scale)
      throws SFException {
    if (binaryFormatter == null) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "missing binary formatter");
    }

    if (binaryFormatter == SFBinaryFormat.HEX) {
      // Shortcut: the values are already passed with hex encoding, so just
      // return the string unchanged rather than constructing an SFBinary.
      return obj.toString();
    }

    SFBinary sfb =
        new SFBinary(
            converters.getBytesConverter().getBytes(obj, columnType, columnSubType, scale));
    return binaryFormatter.format(sfb);
  }
}
