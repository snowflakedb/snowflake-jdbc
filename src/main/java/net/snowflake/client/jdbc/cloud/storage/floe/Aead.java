package net.snowflake.client.jdbc.cloud.storage.floe;

import net.snowflake.client.jdbc.cloud.storage.floe.aead.Gcm;

public enum Aead {
  // TODO confirm id
  AES_GCM_256((byte) 0, "AES/GCM/NoPadding", 32, 12, 16, new Gcm(16)),
  AES_GCM_128((byte) 1, "AES/GCM/NoPadding", 16, 12, 16, new Gcm(16));

  private byte id;
  private String jceName;
  private int keyLength;
  private int ivLength;
  private int authTagLength;
  private AeadProvider aeadProvider;

  Aead(
      byte id,
      String jceName,
      int keyLength,
      int ivLength,
      int authTagLength,
      AeadProvider aeadProvider) {
    this.jceName = jceName;
    this.keyLength = keyLength;
    this.id = id;
    this.ivLength = ivLength;
    this.authTagLength = authTagLength;
    this.aeadProvider = aeadProvider;
  }

  byte getId() {
    return id;
  }

  String getJceName() {
    return jceName;
  }

  int getKeyLength() {
    return keyLength;
  }

  int getIvLength() {
    return ivLength;
  }

  int getAuthTagLength() {
    return authTagLength;
  }

  AeadProvider getAeadProvider() {
    return aeadProvider;
  }
}
