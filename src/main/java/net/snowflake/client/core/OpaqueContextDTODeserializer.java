package net.snowflake.client.core;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;


public class OpaqueContextDTODeserializer extends JsonDeserializer<OpaqueContextDTO> {
    @Override
    public OpaqueContextDTO deserialize(JsonParser p, DeserializationContext ctxt) {
        // Implement custom deserialization logic in the future when OpaqueContextDTO is not empty
        return new OpaqueContextDTO();
    }
}