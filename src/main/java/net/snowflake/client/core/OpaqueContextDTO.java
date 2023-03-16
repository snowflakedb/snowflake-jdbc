package net.snowflake.client.core;

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
