package net.snowflake.client.core;
public class OpaqueContextDTO {
  // In the future, we may store some context information which should be opaque to JDBC side using Base64 encoding.
  byte[] base64Data;

  public OpaqueContextDTO(byte[] base64Data) {
    this.base64Data = base64Data;
  }

  public void setBase64Data(byte[] base64Data) {
    this.base64Data = base64Data;
  }

  public byte[] getBase64Data() {
    return base64Data;
  }
}
