package net.snowflake.client.internal.core;

import java.sql.Types;
import net.snowflake.client.api.resultset.SnowflakeType;

public class ColumnTypeHelper {
  public static int getColumnType(int internalColumnType, SFBaseSession session) {
    int externalColumnType = internalColumnType;

    if (internalColumnType == SnowflakeType.EXTRA_TYPES_TIMESTAMP_LTZ) {
      externalColumnType = Types.TIMESTAMP;
    } else if (internalColumnType == SnowflakeType.EXTRA_TYPES_TIMESTAMP_TZ) {
      externalColumnType =
          session == null
              ? Types.TIMESTAMP_WITH_TIMEZONE
              : session.getEnableReturnTimestampWithTimeZone()
                  ? Types.TIMESTAMP_WITH_TIMEZONE
                  : Types.TIMESTAMP;
    }
    return externalColumnType;
  }
}
