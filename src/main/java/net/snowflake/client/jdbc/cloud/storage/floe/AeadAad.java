package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;

class AeadAad {
  private final byte[] bytes;

  private AeadAad(long segmentCounter, byte terminalityByte) {
    ByteBuffer buf = ByteBuffer.allocate(9);
    buf.putLong(segmentCounter);
    buf.put(terminalityByte);
    this.bytes = buf.array();
  }

  static AeadAad nonTerminal(long segmentCounter) {
    return new AeadAad(segmentCounter, (byte) 0);
  }

  byte[] getBytes() {
    return bytes;
  }
}
