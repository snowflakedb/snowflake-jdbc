package net.snowflake.client.jdbc.cloud.storage.floe;

import java.util.function.Supplier;
import net.snowflake.client.jdbc.cloud.storage.floe.aead.AeadProvider;
import net.snowflake.client.jdbc.cloud.storage.floe.aead.Gcm;

public enum Aead {
  AES_GCM_256((byte) 0, "AES", "AES/GCM/NoPadding", 32, 12, 16, () -> new Gcm(16));

  private byte id;
  private String jceKeyTypeName;
  private String jceFullName;
  private int keyLength;
  private int ivLength;
  private int authTagLength;
  private Supplier<AeadProvider> aeadProvider;

  Aead(
      byte id,
      String jceKeyTypeName,
      String jceFullName,
      int keyLength,
      int ivLength,
      int authTagLength,
      Supplier<AeadProvider> aeadProvider) {
    this.jceKeyTypeName = jceKeyTypeName;
    this.jceFullName = jceFullName;
    this.keyLength = keyLength;
    this.id = id;
    this.ivLength = ivLength;
    this.authTagLength = authTagLength;
    this.aeadProvider = aeadProvider;
  }

  byte getId() {
    return id;
  }

  public String getJceKeyTypeName() {
    return jceKeyTypeName;
  }

  String getJceFullName() {
    return jceFullName;
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
    return aeadProvider.get();
  }
}
