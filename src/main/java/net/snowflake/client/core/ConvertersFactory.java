package net.snowflake.client.core;

import net.snowflake.client.core.json.Converters;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1;

@SnowflakeJdbcInternalApi
public class ConvertersFactory {
  public static Converters createJsonConverters(
      SFBaseSession session, SnowflakeResultSetSerializableV1 resultSetSerializable) {
    return new Converters(
        resultSetSerializable.getTimeZone(),
        session,
        resultSetSerializable.getResultVersion(),
        resultSetSerializable.isHonorClientTZForTimestampNTZ(),
        resultSetSerializable.getTreatNTZAsUTC(),
        resultSetSerializable.getUseSessionTimezone(),
        resultSetSerializable.getFormatDateWithTimeZone(),
        resultSetSerializable.getBinaryFormatter(),
        resultSetSerializable.getDateFormatter(),
        resultSetSerializable.getTimeFormatter(),
        resultSetSerializable.getTimestampNTZFormatter(),
        resultSetSerializable.getTimestampLTZFormatter(),
        resultSetSerializable.getTimestampTZFormatter());
  }
}
