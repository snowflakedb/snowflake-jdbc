package net.snowflake.client.core.arrow;

import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.util.JsonStringHashMap;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

public class StructuredTypeDateTimeConverter {

    private final TimeZone sessionTimeZone;
    private final SFBaseSession session;
    private final long resultVersion;
    private final boolean honorClientTZForTimestampNTZ;
    private final boolean treatNTZAsUTC;
    private final boolean useSessionTimezone;
    private final boolean formatDateWithTimeZone;

    public StructuredTypeDateTimeConverter(
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

    public Timestamp getTimestamp(JsonStringHashMap<String, Object> obj, SnowflakeType type, TimeZone tz, int scale) throws SFException {
        if (tz == null) {
            tz = TimeZone.getDefault();
        }
        switch (type) {
            case TIMESTAMP_LTZ:
                if (obj.values().size() == 2) {
                    return TwoFieldStructToTimestampLTZConverter.getTimestamp(
                            (long) obj.get("epoch"),
                            (int) obj.get("fraction"),
                            false,
                            sessionTimeZone,
                            useSessionTimezone
                    );
                }
                break;
            case TIMESTAMP_NTZ:
                if (obj.values().size() == 2) {
                    return TwoFieldStructToTimestampNTZConverter.getTimestamp(
                            (long) obj.get("epoch"),
                            (int) obj.get("fraction"),
                            false,
                            tz,
                            sessionTimeZone,
                            treatNTZAsUTC,
                            useSessionTimezone,
                            honorClientTZForTimestampNTZ
                    );
                }
                break;
            case TIMESTAMP_TZ:
                if (obj.values().size() == 2) {
                    return TwoFieldStructToTimestampTZConverter.getTimestamp(
                            (long) obj.get("epoch"),
                            (int) obj.get("timezone"),
                            scale
                    );
                } else if (obj.values().size() == 3) {
                    return ThreeFieldStructToTimestampTZConverter.getTimestamp(
                            (long) obj.get("epoch"),
                            (int) obj.get("fraction"),
                            (int) obj.get("timezone"),
                            false,
                            resultVersion,
                            useSessionTimezone
                    );
                }
                break;
        }
        throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, "Unexpected Arrow Field for " + type.name());
    }

    public Date getDate(int value, TimeZone tz) throws SFException {
        return DateConverter.getDate(value, tz, sessionTimeZone, false);
    }

    public Time getTime(long value, int scale) throws SFException {
        return BigIntToTimeConverter.getTime(value, scale, useSessionTimezone);
    }

}
