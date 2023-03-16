package net.snowflake.client.core;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class OpaqueContextDTOSerializer extends JsonSerializer<OpaqueContextDTO> {
    @Override
    public void serialize(OpaqueContextDTO value, JsonGenerator gen, SerializerProvider serializers) {
        // Implement custom serialization logic in the future when OpaqueContextDTO is not empty
    }
}