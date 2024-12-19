package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.charset.StandardCharsets;

public enum FloePurpose {
  HEADER_TAG("HEADER_TAG:".getBytes(StandardCharsets.UTF_8));

  private final byte[] bytes;

  FloePurpose(byte[] bytes) {
    this.bytes = bytes;
  }

  public byte[] getBytes() {
    return bytes;
  }
}
