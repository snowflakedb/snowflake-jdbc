package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;

class AeadIv {
  private final byte[] bytes;

  AeadIv(byte[] bytes) {
    this.bytes = bytes;
  }

  static AeadIv generateRandom(FloeRandom floeRandom, int ivLength) {
    return new AeadIv(floeRandom.ofLength(ivLength));
  }

  static AeadIv from(ByteBuffer buffer, int ivLength) {
    byte[] bytes = new byte[ivLength];
    buffer.get(bytes);
    return new AeadIv(bytes);
  }

  byte[] getBytes() {
    return bytes;
  }
}
