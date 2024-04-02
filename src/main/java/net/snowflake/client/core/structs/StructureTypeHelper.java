package net.snowflake.client.core.structs;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class StructureTypeHelper {
  private static final String STRUCTURED_TYPE_ENABLED_PROPERTY_NAME = "STRUCTURED_TYPE_ENABLED";
  private static boolean structuredTypeEnabled =
          Boolean.valueOf(System.getProperty(STRUCTURED_TYPE_ENABLED_PROPERTY_NAME));

  public static boolean isStructureTypeEnabled() {
    return structuredTypeEnabled;
  }

  public static void enableStructuredType() {
    structuredTypeEnabled = true;
  }

  public static void disableStructuredType() {
    structuredTypeEnabled = false;
  }
}
