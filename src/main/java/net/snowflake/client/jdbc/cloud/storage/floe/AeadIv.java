package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;

class AeadIv {
  private final byte[] bytes;

  AeadIv(byte[] bytes) {
    this.bytes = bytes;
  }

  public static AeadIv generateRandom(FloeRandom floeRandom, int ivLength) {
    return new AeadIv(floeRandom.ofLength(ivLength));
  }

  public static AeadIv from(ByteBuffer buffer, int ivLength) {
    byte[] bytes = new byte[ivLength];
    buffer.get(bytes);
    return new AeadIv(bytes);
  }

  byte[] getBytes() {
    return bytes;
  }
}
