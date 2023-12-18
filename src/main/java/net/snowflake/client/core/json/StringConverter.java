package net.snowflake.client.core.json;

import java.sql.Date;
import java.sql.Types;
import java.util.TimeZone;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.*;

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
      Converters converters) {
    this.sessionTimeZone = sessionTimeZone;
    this.binaryFormatter = binaryFormatter;
    this.dateFormatter = dateFormatter;
    this.timeFormatter = timeFormatter;
    this.timestampNTZFormatter = timestampNTZFormatter;
    this.timestampLTZFormatter = timestampLTZFormatter;
    this.timestampTZFormatter = timestampTZFormatter;
    this.resultVersion = resultVersion;
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
        SFTimestamp sfTS =
            ResultUtil.getSFTimestamp(
                obj.toString(), scale, columnSubType, resultVersion, sessionTimeZone);

        String timestampStr =
            ResultUtil.getSFTimestampAsString(
                sfTS,
                columnType,
                scale,
                timestampNTZFormatter,
                timestampLTZFormatter,
                timestampTZFormatter);

        logger.debug(
            "Converting timestamp to string from: {} to: {}",
            (ArgSupplier) obj::toString,
            timestampStr);

        return timestampStr;

      case Types.DATE:
        Date date =
            converters
                .getDateTimeConverter()
                .getDate(obj, columnType, columnSubType, TimeZone.getDefault(), scale);

        if (dateFormatter == null) {
          throw new SFException(ErrorCode.INTERNAL_ERROR, "missing date formatter");
        }

        String dateStr = ResultUtil.getDateAsString(date, dateFormatter);

        logger.debug(
            "Converting date to string from: {} to: {}", (ArgSupplier) obj::toString, dateStr);

        return dateStr;

      case Types.TIME:
        SFTime sfTime = ResultUtil.getSFTime(obj.toString(), scale);

        if (timeFormatter == null) {
          throw new SFException(ErrorCode.INTERNAL_ERROR, "missing time formatter");
        }

        String timeStr = ResultUtil.getSFTimeAsString(sfTime, scale, timeFormatter);

        logger.debug(
            "Converting time to string from: {} to: {}", (ArgSupplier) obj::toString, timeStr);

        return timeStr;

      case Types.BINARY:
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

      default:
        break;
    }

    return obj.toString();
  }
}
