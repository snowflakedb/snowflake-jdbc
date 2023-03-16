package net.snowflake.client.core;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public class OpaqueContextDTO {
  // currently empty

  public static class CustomSerializer extends JsonSerializer<OpaqueContextDTO> {
    @Override
    public void serialize(OpaqueContextDTO value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      // Add custom serialization logic here in the future when we add new fields into OpaqueContextDTO
      gen.writeEndObject();
    }
  }

  public static class CustomDeserializer extends JsonDeserializer<OpaqueContextDTO> {
    @Override
    public OpaqueContextDTO deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      // Add custom deserialization logic here in the future when we add new fields into OpaqueContextDTO
      return new OpaqueContextDTO();
    }
  }
}
