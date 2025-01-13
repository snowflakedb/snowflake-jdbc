package net.snowflake.client.jdbc.cloud.storage.floe;

public enum Hash {
  SHA384((byte) 0, "HmacSHA384");

  private byte id;
  private final String jceName;

  Hash(byte id, String jceName) {
    this.id = id;
    this.jceName = jceName;
  }

  byte getId() {
    return id;
  }

  public String getJceName() {
    return jceName;
  }
}
