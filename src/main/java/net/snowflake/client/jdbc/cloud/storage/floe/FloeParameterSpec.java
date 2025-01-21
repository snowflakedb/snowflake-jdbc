package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FloeParameterSpec {
  private final Aead aead;
  private final Hash hash;
  private final int encryptedSegmentLength;
  private final FloeIvLength floeIvLength;
  private final FloeRandom floeRandom;
  private final int keyRotationModulo;
  private final long maxSegmentNumber;

  public FloeParameterSpec(Aead aead, Hash hash, int encryptedSegmentLength, int floeIvLength) {
    this(
        aead,
        hash,
        encryptedSegmentLength,
        new FloeIvLength(floeIvLength),
        new SecureFloeRandom(),
        1 << 20,
        1L << 40);
  }

  FloeParameterSpec(
      Aead aead,
      Hash hash,
      int encryptedSegmentLength,
      FloeIvLength floeIvLength,
      FloeRandom floeRandom,
      int keyRotationModulo,
      long maxSegmentNumber) {
    this.aead = aead;
    this.hash = hash;
    this.encryptedSegmentLength = encryptedSegmentLength;
    this.floeIvLength = floeIvLength;
    this.floeRandom = floeRandom;
    this.keyRotationModulo = keyRotationModulo;
    this.maxSegmentNumber = maxSegmentNumber;
    if (encryptedSegmentLength <= 0) {
      throw new IllegalArgumentException("encryptedSegmentLength must be > 0");
    }
    if (floeIvLength.getLength() <= 0) {
      throw new IllegalArgumentException("floeIvLength must be > 0");
    }
  }

  byte[] paramEncode() {
    ByteBuffer result = ByteBuffer.allocate(10).order(ByteOrder.BIG_ENDIAN);
    result.put(aead.getId());
    result.put(hash.getId());
    result.putInt(encryptedSegmentLength);
    result.putInt(floeIvLength.getLength());
    return result.array();
  }

  public Aead getAead() {
    return aead;
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

  int getEncryptedSegmentLength() {
    return encryptedSegmentLength;
  }

  int getPlainTextSegmentLength() {
    // sizeof(int) == 4, file size is a part of the segment ciphertext
    return encryptedSegmentLength - aead.getIvLength() - aead.getAuthTagLength() - 4;
  }

  int getKeyRotationModulo() {
    return keyRotationModulo;
  }

  long getMaxSegmentNumber() {
    return maxSegmentNumber;
  }
}
