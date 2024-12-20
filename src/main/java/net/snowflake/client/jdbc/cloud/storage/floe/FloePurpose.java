package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

interface FloePurpose {
  byte[] generate();
}

class HeaderTagFloePurpose implements FloePurpose {
  private static final byte[] bytes = "HEADER_TAG:".getBytes(StandardCharsets.UTF_8);

  static final HeaderTagFloePurpose INSTANCE = new HeaderTagFloePurpose();

  private HeaderTagFloePurpose() {}

  @Override
  public byte[] generate() {
    return bytes;
  }
}

class DekTagFloePurpose implements FloePurpose {
  private static final byte[] prefix = "DEK:".getBytes(StandardCharsets.UTF_8);

  private final byte[] bytes;

  DekTagFloePurpose(long segmentCount) {
    ByteBuffer buffer = ByteBuffer.allocate(prefix.length + 8 /*size of long*/);
    buffer.put(prefix);
    buffer.putLong(segmentCount);
    this.bytes = buffer.array();
  }

  @Override
  public byte[] generate() {
    return bytes;
  }
}
