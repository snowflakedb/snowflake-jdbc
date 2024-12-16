package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FloeParameterSpec {
  private final Aead aead;
  private final Hash hash;
  private final int encryptedSegmentLength;
  private final FloeIvLength floeIvLength;
  private final FloeRandom floeRandom;

  public FloeParameterSpec(Aead aead, Hash hash, int encryptedSegmentLength, int floeIvLength) {
    this(
        aead, hash, encryptedSegmentLength, new FloeIvLength(floeIvLength), new SecureFloeRandom());
  }

  FloeParameterSpec(
      Aead aead,
      Hash hash,
      int encryptedSegmentLength,
      FloeIvLength floeIvLength,
      FloeRandom floeRandom) {
    this.aead = aead;
    this.hash = hash;
    this.encryptedSegmentLength = encryptedSegmentLength;
    this.floeIvLength = floeIvLength;
    this.floeRandom = floeRandom;
  }

  byte[] paramEncode() {
    ByteBuffer result = ByteBuffer.allocate(10).order(ByteOrder.BIG_ENDIAN);
    result.put(aead.getId());
    result.put(hash.getId());
    result.putInt(encryptedSegmentLength);
    result.putInt(floeIvLength.getLength());
    return result.array();
  }

  public Hash getHash() {
    return hash;
  }

  public FloeIvLength getFloeIvLength() {
    return floeIvLength;
  }

  FloeRandom getFloeRandom() {
    return floeRandom;
  }
}
