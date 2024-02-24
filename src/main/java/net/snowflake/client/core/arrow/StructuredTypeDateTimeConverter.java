package net.snowflake.client.core.arrow;

import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;

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

    public Timestamp getTimestamp(Object obj, int columnType, int columnSubType, TimeZone tz, int scale) throws SFException {
        if (obj == null) {
            return null;
        }
        return null;
    }
}
