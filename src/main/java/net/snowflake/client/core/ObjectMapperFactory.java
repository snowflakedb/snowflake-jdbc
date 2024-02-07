package net.snowflake.client.core;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Factor method used to create ObjectMapper instance. All object mapper in JDBC should be created
 * by this method.
 */
public class ObjectMapperFactory {
  static final SFLogger logger = SFLoggerFactory.getLogger(ObjectMapperFactory.class);

  // Snowflake allows up to 16M string size and returns base64 encoded value that makes it up to 23M
  public static final int DEFAULT_MAX_JSON_STRING_LEN = 23_000_000;
  public static final String MAX_JSON_STRING_LENGTH_JVM =
      "net.snowflake.jdbc.objectMapper.maxJsonStringLength";

  public static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(MapperFeature.OVERRIDE_PUBLIC_ACCESS_MODIFIERS, false);
    mapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);

    int maxJsonStringLength = DEFAULT_MAX_JSON_STRING_LEN;
    String maxJsonStringLengthProperty =
        SnowflakeUtil.systemGetProperty(MAX_JSON_STRING_LENGTH_JVM);
    if (!Strings.isNullOrEmpty(maxJsonStringLengthProperty)) {
      try {
        maxJsonStringLength = Integer.parseInt(maxJsonStringLengthProperty);
      } catch (NumberFormatException ex) {
        // log the exception
        logger.error("Could not parse maxJsonStringLength", ex);
      }
    }
    mapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(maxJsonStringLength).build());
    return mapper;
  }
}
