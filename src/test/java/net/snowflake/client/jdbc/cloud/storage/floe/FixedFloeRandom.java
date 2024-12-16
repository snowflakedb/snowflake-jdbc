package net.snowflake.client.jdbc.cloud.storage.floe;

public class FixedFloeRandom implements FloeRandom {
  private final byte[] bytes;

  public FixedFloeRandom(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public byte[] ofLength(int length) {
    if (bytes.length != length) {
      throw new IllegalArgumentException("allowed only " + bytes.length + " bytes");
    }
    return bytes;
  }
}
