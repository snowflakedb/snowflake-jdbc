package net.snowflake.client.core;


import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Factor method used to create ObjectMapper instance. All object mapper in JDBC should be created
 * by this method.
 */
public class ObjectMapperFactory {
  @SnowflakeJdbcInternalApi
  // Snowflake allows up to 16M string size and returns base64 encoded value that makes it up to 23M
  public static final int DEFAULT_MAX_JSON_STRING_LEN = 23_000_000;

  public static final String MAX_JSON_STRING_LENGTH_JVM =
      "net.snowflake.jdbc.objectMapper.maxJsonStringLength";

  private static final SFLogger logger = SFLoggerFactory.getLogger(ObjectMapperFactory.class);

  public static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(MapperFeature.OVERRIDE_PUBLIC_ACCESS_MODIFIERS, false);
    mapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);

    // override the maxStringLength value in ObjectMapper
    int maxJsonStringLength =
        SystemUtil.convertSystemPropertyToIntValue(
            MAX_JSON_STRING_LENGTH_JVM, DEFAULT_MAX_JSON_STRING_LEN);
    mapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(maxJsonStringLength).build());
    return mapper;
  }
}
