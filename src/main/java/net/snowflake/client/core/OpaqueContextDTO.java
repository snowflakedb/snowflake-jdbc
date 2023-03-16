package net.snowflake.client.core;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

// @JsonSerialize(using = OpaqueContextDTOSerializer.class)
// @JsonDeserialize(using = OpaqueContextDTODeserializer.class)
public class OpaqueContextDTO {
    private int dummy = 0;  // add this dummy field to avoid empty class Jackson serialization/deserialization error

    // currently empty
    public OpaqueContextDTO() {

    }

    public int getDummy() {
        return dummy;
    }

    public void setDummy(int dummy) {
        this.dummy = dummy;
    }
}
