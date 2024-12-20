package net.snowflake.client.jdbc.cloud.storage.floe;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

abstract class BaseSegmentProcessor {
  protected static final int NON_TERMINAL_SEGMENT_SIZE_MARKER = -1;
  protected static final int headerTagLength = 32;

  protected final FloeParameterSpec parameterSpec;
  protected final FloeKey floeKey;
  protected final FloeAad floeAad;

  protected final KeyDerivator keyDerivator;

  private AeadKey currentAeadKey;

  BaseSegmentProcessor(FloeParameterSpec parameterSpec, FloeKey floeKey, FloeAad floeAad) {
    this.parameterSpec = parameterSpec;
    this.floeKey = floeKey;
    this.floeAad = floeAad;
    this.keyDerivator = new KeyDerivator(parameterSpec);
  }

  protected AeadKey getKey(FloeKey floeKey, FloeIv floeIv, FloeAad floeAad, long segmentCounter) {
    if (currentAeadKey == null || segmentCounter % parameterSpec.getKeyRotationModulo() == 0) {
      currentAeadKey = deriveKey(floeKey, floeIv, floeAad, segmentCounter);
    }
    return currentAeadKey;
  }

  private AeadKey deriveKey(FloeKey floeKey, FloeIv floeIv, FloeAad floeAad, long segmentCounter) {
    byte[] keyBytes =
        keyDerivator.hkdfExpand(
            floeKey,
            floeIv,
            floeAad,
            new DekTagFloePurpose(segmentCounter),
            parameterSpec.getAead().getKeyLength());
    SecretKey key =
        new SecretKeySpec(keyBytes, "AES"); // for now it is safe as we use only AES as AEAD
    return new AeadKey(key);
  }
}
