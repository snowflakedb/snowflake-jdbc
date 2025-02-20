package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;

public class IncrementingFloeRandom implements FloeRandom {
  private int seed;

  public IncrementingFloeRandom(int seed) {
    this.seed = seed;
  }

  @Override
  public byte[] ofLength(int length) {
    ByteBuffer buffer = ByteBuffer.allocate(length);
    buffer.putInt(seed++);
    return buffer.array();
  }
}
