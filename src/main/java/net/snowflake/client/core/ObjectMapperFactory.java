package net.snowflake.client.core;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Factor method used to create ObjectMapper instance. All object mapper in JDBC should be created
 * by this method.
 */
public class ObjectMapperFactory {
  public static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(MapperFeature.OVERRIDE_PUBLIC_ACCESS_MODIFIERS, false);
    mapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);

    return mapper;
  }
}
