package net.snowflake.client.jdbc.cloud.storage.floe;

public enum Aead {
  AES_GCM_128((byte) 0),
  AES_GCM_256((byte) 1);

  private byte id;

  Aead(byte id) {
    this.id = id;
  }

  byte getId() {
    return id;
  }
}
