package net.snowflake.client.core;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Factor method used to create ObjectMapper instance. All object mapper in JDBC should be created
 * by this method.
 */
public class ObjectMapperFactory {
  // Snowflake allows up to 16M string size and returns base64 encoded value that makes it up to 23M
  public static final int DEFAULT_MAX_JSON_STRING_LEN = 23_000_000;
  private static int maxJsonStringLength = DEFAULT_MAX_JSON_STRING_LEN;

  public static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(MapperFeature.OVERRIDE_PUBLIC_ACCESS_MODIFIERS, false);
    mapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);

    mapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(maxJsonStringLength).build());
    return mapper;
  }

  /**
   * Update the max string length used in StreamReadConstraints. Only need to set this once when
   * parsing the maxJsonStringLength setting.
   *
   * @param maxJsonStringLength the max string length
   */
  public static void setMaxJsonStringLength(int maxJsonStringLength) {
    ObjectMapperFactory.maxJsonStringLength = maxJsonStringLength;
  }
}
