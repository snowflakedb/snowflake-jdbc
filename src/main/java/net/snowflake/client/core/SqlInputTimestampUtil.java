package net.snowflake.client.core;

import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

import java.sql.Timestamp;
import java.sql.Types;

public class SqlInputTimestampUtil {

    public static SnowflakeDateTimeFormat extractDateTimeFormat(SFBaseSession session, String format) {
        return SnowflakeDateTimeFormat.fromSqlFormat(
                (String) session.getCommonParameters().get(format));
    }

    public static Timestamp getTimestampFromType(int columnSubType, String value, SFBaseSession session) {
        if (columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ) {
            return getTimestampFromFormat("TIMESTAMP_LTZ_OUTPUT_FORMAT", value, session);
        } else if (columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_NTZ
                || columnSubType == Types.TIMESTAMP) {
            return getTimestampFromFormat("TIMESTAMP_NTZ_OUTPUT_FORMAT", value, session);
        } else if (columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ) {
            return getTimestampFromFormat("TIMESTAMP_TZ_OUTPUT_FORMAT", value, session);
        } else {
            return null;
        }
    }

    private static Timestamp getTimestampFromFormat(String format, String value, SFBaseSession session) {
        String rawFormat = (String) session.getCommonParameters().get(format);
        if (rawFormat == null || rawFormat.isEmpty()) {
            rawFormat = (String) session.getCommonParameters().get("TIMESTAMP_OUTPUT_FORMAT");
        }
        SnowflakeDateTimeFormat formatter = SnowflakeDateTimeFormat.fromSqlFormat(rawFormat);
        return formatter.parse(value).getTimestamp();
    }

}
