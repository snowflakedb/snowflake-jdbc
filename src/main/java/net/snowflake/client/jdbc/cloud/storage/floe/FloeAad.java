package net.snowflake.client.jdbc.cloud.storage.floe;

import java.util.Optional;

class FloeAad {
  private final byte[] aad;

  FloeAad(byte[] aad) {
    this.aad = Optional.ofNullable(aad).orElse(new byte[0]);
  }

  byte[] getBytes() {
    return aad;
  }
}
