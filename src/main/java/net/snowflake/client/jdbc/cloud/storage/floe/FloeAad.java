package net.snowflake.client.jdbc.cloud.storage.floe;

import java.util.Optional;

class FloeAad {
  private static final byte[] EMPTY_AAD = new byte[0];
  private final byte[] aad;

  FloeAad(byte[] aad) {
    this.aad = Optional.ofNullable(aad).orElse(EMPTY_AAD);
  }

  byte[] getBytes() {
    return aad;
  }
}
