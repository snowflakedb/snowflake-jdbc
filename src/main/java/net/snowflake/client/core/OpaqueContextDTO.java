package net.snowflake.client.core;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
@JsonSerialize(using = OpaqueContextDTOSerializer.class)
@JsonDeserialize(using = OpaqueContextDTODeserializer.class)
public class OpaqueContextDTO {
    // currently empty
    public OpaqueContextDTO() {

    }
}
